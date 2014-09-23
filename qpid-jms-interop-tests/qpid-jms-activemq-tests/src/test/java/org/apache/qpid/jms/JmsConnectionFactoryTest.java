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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

public class JmsConnectionFactoryTest extends AmqpTestSupport {

    private final String username = "USER";
    private final String password = "PASSWORD";

    protected String getGoodProviderAddress() {
        return getBrokerAmqpConnectionURI().toString();
    }

    protected URI getGoodProviderAddressURI() throws URISyntaxException {
        return new URI(getGoodProviderAddress());
    }

    protected String getBadProviderAddress() {
        return "bad://127.0.0.1:" + 5763;
    }

    protected URI getBadProviderAddressURI() throws URISyntaxException {
        return new URI(getBadProviderAddress());
    }

    @Test(timeout=60000)
    public void testConnectionFactoryCreate() {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        assertNull(factory.getUsername());
        assertNull(factory.getPassword());
    }

    @Test(timeout=60000)
    public void testConnectionFactoryCreateUsernameAndPassword() {
        JmsConnectionFactory factory = new JmsConnectionFactory(username, password);
        assertNotNull(factory.getUsername());
        assertNotNull(factory.getPassword());
        assertEquals(username, factory.getUsername());
        assertEquals(password, factory.getPassword());
    }

    @Test(expected = JMSException.class)
    public void testCreateConnectionBadProviderURI() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBadProviderAddressURI());
        factory.createConnection();
    }

    @Test(expected = JMSException.class)
    public void testCreateConnectionBadProviderString() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBadProviderAddress());
        factory.createConnection();
    }

    @Test(timeout=60000)
    public void testCreateConnectionGoodProviderURI() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getGoodProviderAddressURI());
        Connection connection = factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test(timeout=60000)
    public void testCreateConnectionGoodProviderString() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getGoodProviderAddress());
        Connection connection = factory.createConnection();
        assertNotNull(connection);
        connection.close();
    }

    @Test(timeout=60000)
    public void testUriOptionsApplied() throws Exception {
        String uri = getGoodProviderAddress() + "?jms.omitHost=true&jms.forceAsyncSend=true";
        JmsConnectionFactory factory = new JmsConnectionFactory(uri);
        assertTrue(factory.isOmitHost());
        assertTrue(factory.isForceAsyncSend());
        JmsConnection connection = (JmsConnection) factory.createConnection();
        assertNotNull(connection);
        assertTrue(connection.isOmitHost());
        assertTrue(connection.isForceAsyncSend());
        connection.close();
    }

    @Test(expected=IllegalArgumentException.class)
    public void testBadUriOptionCausesFail() throws Exception {
        String uri = getGoodProviderAddress() + "?jms.omitHost=true&jms.badOption=true";
        new JmsConnectionFactory(uri);
    }
}
