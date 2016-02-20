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

import java.util.UUID;

/**
 * Implements the SASL Scram SHA 256 authentication Mechanism.
 */
public class ScramSHA256Mechanism extends AbstractScramSHAMechanism {

    public static final String SHA_256 = "SHA-256";
    public static final String HMAC_SHA_256 = "HmacSHA256";

    public ScramSHA256Mechanism() {
        this(UUID.randomUUID().toString());
    }

    /** For unit testing */
    ScramSHA256Mechanism(String clientNonce) {
        super(SHA_256, HMAC_SHA_256, clientNonce);
    }

    @Override
    public int getPriority() {
        return PRIORITY.HIGHER.getValue();
    }

    @Override
    public String getName() {
        return "SCRAM-SHA-256";
    }
}
