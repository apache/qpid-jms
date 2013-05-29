/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.engine;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class SaslEngineFactoryImpl implements SaslEngineFactory
{
    private static final String CRAM_MD5 = "CRAM-MD5";
    private static final String PLAIN = "PLAIN";
    private static final String ANONYMOUS = "ANONYMOUS";

    private static final String ASCII = "ASCII";

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static final Collection<String> DEFAULT_MECHS = Arrays.asList(CRAM_MD5, PLAIN, ANONYMOUS);

    private interface PropertyUsingSaslEngine extends SaslEngine
    {
        void setProperties(Map<String,Object> properties);

        boolean isValid();
    }

    abstract public class AbstractUsernamePasswordEngine implements PropertyUsingSaslEngine
    {
        private String _username;
        private String _password;

        public String getUsername()
        {
            return _username;
        }

        public String getPassword()
        {
            return _password;
        }

        @Override
        public void setProperties(Map<String, Object> properties)
        {
            Object user = properties.get(USERNAME_PROPERTY);
            if(user instanceof String)
            {
                _username = (String) user;
            }
            Object pass = properties.get(PASSWORD_PROPERTY);
            if(pass instanceof String)
            {
                _password = (String) pass;
            }
        }

        @Override
        public boolean isValid()
        {
            return _username != null && _password != null;
        }
    }

    public class CRAMMD5Engine extends AbstractUsernamePasswordEngine
    {
        private static final String HMACMD5 = "HMACMD5";

        private boolean _sentResponse;

        @Override
        public String getMechanism()
        {
            return CRAM_MD5;
        }

        @Override
        public byte[] getResponse(byte[] challenge)
        {
            if(!_sentResponse && challenge != null && challenge.length != 0)
            {
                try
                {
                    SecretKeySpec key = new SecretKeySpec(getPassword().getBytes(ASCII), HMACMD5);
                    Mac mac = Mac.getInstance(HMACMD5);
                    mac.init(key);

                    byte[] bytes = mac.doFinal(challenge);

                    StringBuffer hash = new StringBuffer(getUsername());
                    hash.append(' ');
                    for (int i = 0; i < bytes.length; i++)
                    {
                        String hex = Integer.toHexString(0xFF & bytes[i]);
                        if (hex.length() == 1)
                        {
                            hash.append('0');
                        }
                        hash.append(hex);
                    }

                    _sentResponse = true;
                    return hash.toString().getBytes(ASCII);
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new SaslFailureException(e);
                }
                catch (InvalidKeyException e)
                {
                    throw new SaslFailureException(e);
                }
                catch (NoSuchAlgorithmException e)
                {
                    throw new SaslFailureException(e);
                }
            }
            else
            {
                return EMPTY_BYTES;
            }
        }
    }

    public class PlainEngine extends AbstractUsernamePasswordEngine
    {
        private boolean _sentInitialResponse;

        @Override
        public String getMechanism()
        {
            return PLAIN;
        }

        @Override
        public byte[] getResponse(byte[] challenge)
        {
            if(!_sentInitialResponse)
            {
                byte[] usernameBytes = getUsername().getBytes();
                byte[] passwordBytes = getPassword().getBytes();
                byte[] data = new byte[usernameBytes.length+passwordBytes.length+2];
                System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
                System.arraycopy(passwordBytes, 0, data, 2+usernameBytes.length, passwordBytes.length);
                _sentInitialResponse = true;
                return data;
            }

            return EMPTY_BYTES;
        }
    }

    public class AnonymousEngine implements PropertyUsingSaslEngine
    {
        @Override
        public String getMechanism()
        {
            return ANONYMOUS;
        }

        @Override
        public void setProperties(Map<String, Object> properties)
        {
        }

        @Override
        public byte[] getResponse(byte[] challenge)
        {
            return EMPTY_BYTES;
        }

        @Override
        public boolean isValid()
        {
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public SaslEngine createSaslEngine(Map<String,Object> properties, String... mechanisms)
    {
        List<String> mechanismList = Arrays.asList(mechanisms);

        Collection<String> preferredMechs;
        if(properties.get(PREFERRED_MECHANISMS_PROPERTY) instanceof Collection)
        {
            preferredMechs = (Collection<String>) properties.get(PREFERRED_MECHANISMS_PROPERTY);
        }
        else
        {
            preferredMechs = DEFAULT_MECHS;
        }

        PropertyUsingSaslEngine engine = null;
        for(String mech : preferredMechs)
        {
            if(mechanismList.contains(mech))
            {
                if(CRAM_MD5.equals(mech))
                {
                    engine = new CRAMMD5Engine();
                }
                else if(PLAIN.equals(mech))
                {
                    engine = new PlainEngine();
                }
                else if(ANONYMOUS.equals(mech))
                {
                    engine = new AnonymousEngine();
                }
                if(engine != null)
                {
                    engine.setProperties(properties);
                    if(engine.isValid())
                    {
                        break;
                    }
                    else
                    {
                        engine = null;
                    }
                }

            }
        }

        return engine;
    }
}
