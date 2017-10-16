/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.sasl;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;

abstract class AbstractScramSHAMechanism extends AbstractMechanism {
    private static final byte[] INT_1 = new byte[]{0, 0, 0, 1};
    private static final String GS2_HEADER = "n,,";

    private final String clientNonce;
    private final String digestName;
    private final String hmacName;

    private String serverNonce;
    private byte[] salt;
    private int iterationCount;
    private String clientFirstMessageBare;

    private byte[] serverSignature;

    private enum State {
        INITIAL,
        CLIENT_FIRST_SENT,
        CLIENT_PROOF_SENT,
        COMPLETE
    }

    private State state = State.INITIAL;

    AbstractScramSHAMechanism(final String digestName, final String hmacName, final String clientNonce) {
        this.digestName = digestName;
        this.hmacName = hmacName;
        this.clientNonce = clientNonce;
    }

    @Override
    public boolean isApplicable(String username, String password, Principal localPrincipal) {
        return username != null && username.length() > 0 && password != null && password.length() > 0;
    }

    @Override
    public byte[] getInitialResponse() throws SaslException {
        if (state != State.INITIAL) {
            throw new SaslException("Request for initial response not expected in state " + state);
        }
        StringBuilder buf = new StringBuilder("n=");
        buf.append(escapeUsername(saslPrep(getUsername())));
        buf.append(",r=");
        buf.append(clientNonce);
        clientFirstMessageBare = buf.toString();
        state = State.CLIENT_FIRST_SENT;
        return (GS2_HEADER + clientFirstMessageBare).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getChallengeResponse(final byte[] challenge) throws SaslException {
        byte[] response;
        switch (state) {
            case CLIENT_FIRST_SENT:
                response = calculateClientProof(challenge);
                state = State.CLIENT_PROOF_SENT;
                break;
            case CLIENT_PROOF_SENT:
                evaluateOutcome(challenge);
                response = new byte[0];
                state = State.COMPLETE;
                break;
            default:
                throw new SaslException("No challenge expected in state " + state);
        }
        return response;
    }

    @Override
    public void verifyCompletion() throws SaslException {
        super.verifyCompletion();
        if (state != State.COMPLETE) {
            throw new SaslException(String.format("SASL exchange was not fully completed." +
                                                  " Expected state %s but actual state %s",
                                                  State.COMPLETE, state));
        }
    }

    private byte[] calculateClientProof(final byte[] challenge) throws SaslException {
        try {
            String serverFirstMessage = new String(challenge, StandardCharsets.US_ASCII);
            String[] parts = serverFirstMessage.split(",");
            if (parts.length < 3) {
                throw new SaslException("Server challenge '" + serverFirstMessage + "' cannot be parsed");
            } else if (parts[0].startsWith("m=")) {
                throw new SaslException("Server requires mandatory extension which is not supported: " + parts[0]);
            } else if (!parts[0].startsWith("r=")) {
                throw new SaslException("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find nonce");
            }
            String nonce = parts[0].substring(2);
            if (!nonce.startsWith(clientNonce)) {
                throw new SaslException("Server challenge did not use correct client nonce");
            }
            serverNonce = nonce;
            if (!parts[1].startsWith("s=")) {
                throw new SaslException("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find salt");
            }
            String base64Salt = parts[1].substring(2);
            salt = Base64.getDecoder().decode(base64Salt);
            if (!parts[2].startsWith("i=")) {
                throw new SaslException("Server challenge '" + serverFirstMessage + "' cannot be parsed, cannot find iteration count");
            }
            String iterCountString = parts[2].substring(2);
            iterationCount = Integer.parseInt(iterCountString);
            if (iterationCount <= 0) {
                throw new SaslException("Iteration count " + iterationCount + " is not a positive integer");
            }
            byte[] passwordBytes = saslPrep(new String(getPassword())).getBytes(StandardCharsets.UTF_8);
            byte[] saltedPassword = generateSaltedPassword(passwordBytes);


            String clientFinalMessageWithoutProof =
                    "c=" + Base64.getEncoder().encodeToString(GS2_HEADER.getBytes(StandardCharsets.US_ASCII))
                            + ",r=" + serverNonce;

            String authMessage = clientFirstMessageBare
                    + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

            byte[] clientKey = computeHmac(saltedPassword, "Client Key");
            byte[] storedKey = MessageDigest.getInstance(digestName).digest(clientKey);

            byte[] clientSignature = computeHmac(storedKey, authMessage);

            byte[] clientProof = clientKey.clone();
            for (int i = 0; i < clientProof.length; i++) {
                clientProof[i] ^= clientSignature[i];
            }
            byte[] serverKey = computeHmac(saltedPassword, "Server Key");
            serverSignature = computeHmac(serverKey, authMessage);

            String finalMessageWithProof = clientFinalMessageWithoutProof
                    + ",p=" + Base64.getEncoder().encodeToString(clientProof);
            return finalMessageWithProof.getBytes();
        } catch (NoSuchAlgorithmException e) {
            throw new SaslException(e.getMessage(), e);
        }
    }

    private void evaluateOutcome(final byte[] challenge) throws SaslException {
        String serverFinalMessage = new String(challenge, StandardCharsets.US_ASCII);
        String[] parts = serverFinalMessage.split(",");
        if (!parts[0].startsWith("v=")) {
            throw new SaslException("Server final message did not contain verifier");
        }
        byte[] serverSignature = Base64.getDecoder().decode(parts[0].substring(2));
        if (!Arrays.equals(this.serverSignature, serverSignature)) {
            throw new SaslException("Server signature did not match");
        }
    }

    private byte[] computeHmac(final byte[] key, final String string) throws SaslException {
        Mac mac = createHmac(key);
        mac.update(string.getBytes(StandardCharsets.US_ASCII));
        return mac.doFinal();
    }

    private byte[] generateSaltedPassword(final byte[] passwordBytes) throws SaslException {
        Mac mac = createHmac(passwordBytes);

        mac.update(salt);
        mac.update(INT_1);
        byte[] result = mac.doFinal();

        byte[] previous = null;
        for (int i = 1; i < iterationCount; i++) {
            mac.update(previous != null ? previous : result);
            previous = mac.doFinal();
            for (int x = 0; x < result.length; x++) {
                result[x] ^= previous[x];
            }
        }

        return result;
    }

    private Mac createHmac(final byte[] keyBytes) throws SaslException {
        try {
            SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
            Mac mac = Mac.getInstance(hmacName);
            mac.init(key);
            return mac;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new SaslException(e.getMessage(), e);
        }
    }

    private String saslPrep(String name) throws SaslException {
        // TODO - a real implementation of SaslPrep [rfc4013]

        if (!StandardCharsets.US_ASCII.newEncoder().canEncode(name)) {
            throw new SaslException("Can only encode names and passwords which are restricted to ASCII characters");
        }

        return name;
    }

    private String escapeUsername(String name) {
        name = name.replace("=", "=3D");
        name = name.replace(",", "=2C");
        return name;
    }
}
