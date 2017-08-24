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
 * The known good used by these tests is taken from the example in RFC 7677 section 3.
 */
public class ScramSHA256MechanismTest extends AbstractScramSHAMechanismTestBase {

    private static final String USERNAME = "user";
    private static final String PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO";

    private static final byte[] EXPECTED_CLIENT_INITIAL_RESPONSE = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes();
    private static final byte[] SERVER_FIRST_MESSAGE = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".getBytes();
    private static final byte[] EXPECTED_CLIENT_FINAL_MESSAGE = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=".getBytes();
    private static final byte[] SERVER_FINAL_MESSAGE = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".getBytes();

    public ScramSHA256MechanismTest() {
        super(EXPECTED_CLIENT_INITIAL_RESPONSE,
              SERVER_FIRST_MESSAGE,
              EXPECTED_CLIENT_FINAL_MESSAGE,
              SERVER_FINAL_MESSAGE);
    }

    @Override
    protected Mechanism getConfiguredMechanism() {

        Mechanism mech = new ScramSHA256Mechanism(CLIENT_NONCE);
        mech.setUsername(USERNAME);
        mech.setPassword(PASSWORD);
        return mech;
    }

    @Test
    public void testIsEnabledByDefault() {
        ScramSHA256Mechanism mech = new ScramSHA256Mechanism();

        assertTrue("Should be enabled by default", mech.isEnabledByDefault());
    }
}