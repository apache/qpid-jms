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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * The known good used by these tests is taken from the example in RFC 5802 section 5.
 */
public class ScramSHA1MechanismTest extends AbstractScramSHAMechanismTestBase {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "fyko+d2lbbFgONRv9qkxdawL";

    private static final byte[] EXPECTED_CLIENT_INITIAL_RESPONSE = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL".getBytes();
    private static final byte[] SERVER_FIRST_MESSAGE = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes();
    private static final byte[] EXPECTED_CLIENT_FINAL_MESSAGE = "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=".getBytes();
    private static final byte[] SERVER_FINAL_MESSAGE = "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=".getBytes();

    public ScramSHA1MechanismTest() {
        super(EXPECTED_CLIENT_INITIAL_RESPONSE,
              SERVER_FIRST_MESSAGE,
              EXPECTED_CLIENT_FINAL_MESSAGE,
              SERVER_FINAL_MESSAGE);
    }

    @Override
    protected Mechanism getConfiguredMechanism() {

        Mechanism mech = new ScramSHA1Mechanism(CLIENT_NONCE);
        mech.setUsername(USERNAME);
        mech.setPassword(PASSWORD);
        return mech;
    }

    @Test
    public void testIsEnabledByDefault() {
        ScramSHA1Mechanism mech = new ScramSHA1Mechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }
}