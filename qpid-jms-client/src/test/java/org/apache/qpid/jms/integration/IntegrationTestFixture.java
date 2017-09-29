/*
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
 */
package org.apache.qpid.jms.integration;

import static org.junit.Assert.assertNull;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.test.testpeer.TestAmqpPeer;
import org.apache.qpid.proton.amqp.Symbol;

public class IntegrationTestFixture {

    Connection establishConnecton(TestAmqpPeer testPeer) throws JMSException {
        return establishConnecton(testPeer, null, null, null);
    }

    Connection establishConnecton(TestAmqpPeer testPeer, String optionsString) throws JMSException {
        return establishConnecton(testPeer, optionsString, null, null);
    }

    Connection establishConnecton(TestAmqpPeer testPeer, Symbol[] serverCapabilities) throws JMSException {
        return establishConnecton(testPeer, null, serverCapabilities, null);
    }

    Connection establishConnecton(TestAmqpPeer testPeer, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties) throws JMSException {
        return establishConnecton(testPeer, null, serverCapabilities, serverProperties);
    }

    Connection establishConnecton(TestAmqpPeer testPeer, String optionsString, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties) throws JMSException {
        return establishConnecton(testPeer, false, optionsString, serverCapabilities, serverProperties, true);
    }

    Connection establishConnectonWithoutClientID(TestAmqpPeer testPeer, Symbol[] serverCapabilities) throws JMSException {
        return establishConnecton(testPeer, false, null, serverCapabilities, null, false);
    }

    Connection establishConnecton(TestAmqpPeer testPeer, boolean ssl, String optionsString, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties, boolean setClientId) throws JMSException {
        testPeer.expectSaslPlain("guest", "guest");
        testPeer.expectOpen(serverProperties, serverCapabilities);

        // Each connection creates a session for managing temporary destinations etc
        testPeer.expectBegin();

        String remoteURI = buildURI(testPeer, ssl, optionsString);

        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        Connection connection = factory.createConnection("guest", "guest");

        if (setClientId) {
            // Set a clientId to provoke the actual AMQP connection process to occur.
            connection.setClientID("clientName");
        }

        assertNull(testPeer.getThrowable());

        return connection;
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer) throws JMSException {
        return createJMSContext(testPeer, null, null, null);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, int sessionMode) throws JMSException {
        return createJMSContext(testPeer, false, null, null, null, true, sessionMode);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, String optionsString) throws JMSException {
        return createJMSContext(testPeer, optionsString, null, null);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, Symbol[] serverCapabilities) throws JMSException {
        return createJMSContext(testPeer, null, serverCapabilities, null);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties) throws JMSException {
        return createJMSContext(testPeer, null, serverCapabilities, serverProperties);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, String optionsString, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties) throws JMSException {
        return createJMSContext(testPeer, false, optionsString, serverCapabilities, serverProperties, true, JMSContext.AUTO_ACKNOWLEDGE);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, boolean ssl, String optionsString, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties, boolean setClientId) throws JMSException {
        return createJMSContext(testPeer, false, optionsString, serverCapabilities, serverProperties, setClientId, JMSContext.AUTO_ACKNOWLEDGE);
    }

    JMSContext createJMSContext(TestAmqpPeer testPeer, boolean ssl, String optionsString, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties, boolean setClientId, int sessionMode) throws JMSException {
        testPeer.expectSaslPlain("guest", "guest");
        testPeer.expectOpen(serverProperties, serverCapabilities);

        // Each connection creates a session for managing temporary destinations etc
        testPeer.expectBegin();

        String remoteURI = buildURI(testPeer, ssl, optionsString);

        ConnectionFactory factory = new JmsConnectionFactory(remoteURI);
        JMSContext context = factory.createContext("guest", "guest", sessionMode);

        if (setClientId) {
            // Set a clientId to provoke the actual AMQP connection process to occur.
            context.setClientID("clientName");
        }

        assertNull(testPeer.getThrowable());

        return context;
    }

    String buildURI(TestAmqpPeer testPeer, boolean ssl, String optionsString) {
        String scheme = ssl ? "amqps" : "amqp";
        final String baseURI = scheme + "://localhost:" + testPeer.getServerPort();
        String remoteURI = baseURI;
        if (optionsString != null) {
            if (optionsString.startsWith("?")) {
                remoteURI = baseURI + optionsString;
            } else {
                remoteURI = baseURI + "?" + optionsString;
            }
        }

        return remoteURI;
    }
}
