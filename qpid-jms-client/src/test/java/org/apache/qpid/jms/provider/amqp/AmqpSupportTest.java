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
package org.apache.qpid.jms.provider.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.provider.ProviderRedirectedException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.junit.Test;

public class AmqpSupportTest {

    @Test
    public void testCreateRedirectionException() {
        ErrorCondition condition = new ErrorCondition();

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "5672");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost.localdomain");
        info.put(AmqpSupport.NETWORK_HOST, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        condition.setInfo(info);

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        Exception result = AmqpSupport.createRedirectException(error, message, condition);

        assertNotNull(result);
        assertTrue(result instanceof ProviderRedirectedException);

        ProviderRedirectedException pre = (ProviderRedirectedException) result;

        assertEquals(5672, pre.getPort());
        assertEquals("localhost.localdomain", pre.getHostname());
        assertEquals("localhost", pre.getNetworkHost());
        assertEquals("amqp", pre.getScheme());
        assertEquals("websocket", pre.getPath());
    }

    @Test
    public void testCreateRedirectionExceptionWithNoRedirectInfo() {
        ErrorCondition condition = new ErrorCondition();
        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        Exception result = AmqpSupport.createRedirectException(error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderRedirectedException);
        assertTrue(result instanceof IOException);
    }

    @Test
    public void testCreateRedirectionExceptionWithNoNetworkHost() {
        ErrorCondition condition = new ErrorCondition();

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "5672");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        condition.setInfo(info);

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        Exception result = AmqpSupport.createRedirectException(error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderRedirectedException);
        assertTrue(result instanceof IOException);
    }

    @Test
    public void testCreateRedirectionExceptionWithEmptyNetworkHost() {
        ErrorCondition condition = new ErrorCondition();

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "5672");
        info.put(AmqpSupport.NETWORK_HOST, "");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        condition.setInfo(info);

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        Exception result = AmqpSupport.createRedirectException(error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderRedirectedException);
        assertTrue(result instanceof IOException);
    }

    @Test
    public void testCreateRedirectionExceptionWithInvalidPort() {
        ErrorCondition condition = new ErrorCondition();

        Map<Symbol, Object> info = new HashMap<>();
        info.put(AmqpSupport.PORT, "L5672");
        info.put(AmqpSupport.OPEN_HOSTNAME, "localhost");
        info.put(AmqpSupport.NETWORK_HOST, "localhost");
        info.put(AmqpSupport.SCHEME, "amqp");
        info.put(AmqpSupport.PATH, "websocket");

        condition.setInfo(info);

        Symbol error = AmqpError.INTERNAL_ERROR;
        String message = "Failed to connect";

        Exception result = AmqpSupport.createRedirectException(error, message, condition);

        assertNotNull(result);
        assertFalse(result instanceof ProviderRedirectedException);
        assertTrue(result instanceof IOException);
    }
}
