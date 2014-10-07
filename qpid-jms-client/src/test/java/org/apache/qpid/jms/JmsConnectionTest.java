/**
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
package org.apache.qpid.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.util.IdGenerator;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test basic functionality around JmsConnection
 */
public class JmsConnectionTest {

    private final Provider provider = Mockito.mock(Provider.class);
    private final IdGenerator clientIdGenerator = new IdGenerator();

    @Test(expected=JMSException.class)
    public void testJmsConnectionThrowsJMSExceptionProviderStartFails() throws JMSException, IllegalStateException, IOException {
        Mockito.doThrow(IOException.class).when(provider).start();
        new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
    }

    @Test
    public void testGetProvider() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        assertSame(provider, connection.getProvider());
    }

    @Test(expected=javax.jms.IllegalStateException.class)
    public void testSetClientIdWithNull() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        connection.setClientID(null);
    }

    @Test
    public void testGetConnectionId() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        assertEquals("ID:TEST:1", connection.getConnectionId().toString());
    }

    @Test
    public void testAddConnectionListener() throws JMSException {
        JmsConnection connection = new JmsConnection("ID:TEST:1", provider, clientIdGenerator);
        JmsConnectionListener listener = new JmsConnectionListener() {

            @Override
            public void onMessage(JmsInboundMessageDispatch envelope) {
            }

            @Override
            public void onConnectionRestored(URI remoteURI) {
            }

            @Override
            public void onConnectionInterrupted(URI remoteURI) {
            }

            @Override
            public void onConnectionFailure(Throwable error) {
            }

            @Override
            public void onConnectionEstablished(URI remoteURI) {
            }
        };

        assertFalse(connection.removeConnectionListener(listener));
        connection.addConnectionListener(listener);
        assertTrue(connection.removeConnectionListener(listener));
    }
}
