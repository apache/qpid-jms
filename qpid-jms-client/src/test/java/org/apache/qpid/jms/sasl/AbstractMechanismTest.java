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

import static org.junit.Assert.assertEquals;

import java.security.Principal;

import javax.security.sasl.SaslException;

import org.junit.Test;

public class AbstractMechanismTest {

    @Test
    public void testCompareTo() {
        Mechanism mech1 = new Mechanism(1);
        Mechanism mech2 = new Mechanism(2);

        assertEquals(-1, mech1.compareTo(mech2));
        assertEquals(0, mech1.compareTo(mech1));
        assertEquals(1, mech2.compareTo(mech1));
    }

    private static class Mechanism extends AbstractMechanism {

        private int prority;

        public Mechanism(int priority) {
            this.prority = priority;
        }

        @Override
        public int getPriority() {
            return prority;
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        @Override
        public byte[] getInitialResponse() throws SaslException {
            return null;
        }

        @Override
        public byte[] getChallengeResponse(byte[] challenge) throws SaslException {
            return null;
        }

        @Override
        public boolean isApplicable(String username, String password, Principal localPrincipal) {
            return false;
        }

        @Override
        public boolean isEnabledByDefault() {
            return false;
        }
    }
}
