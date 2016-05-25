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
package org.apache.qpid.jms.transports;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

/**
 * Test for class TransportOptions
 */
public class TransportOptionsTest extends QpidJmsTestCase {

    public static final int TEST_SEND_BUFFER_SIZE = 128 * 1024;
    public static final int TEST_RECEIVE_BUFFER_SIZE = TEST_SEND_BUFFER_SIZE;
    public static final int TEST_TRAFFIC_CLASS = 1;
    public static final boolean TEST_TCP_NO_DELAY = false;
    public static final boolean TEST_TCP_KEEP_ALIVE = true;
    public static final int TEST_SO_LINGER = Short.MAX_VALUE;
    public static final int TEST_SO_TIMEOUT = 10;
    public static final int TEST_CONNECT_TIMEOUT = 90000;
    public static final int TEST_DEFAULT_TCP_PORT = 5682;

    @Test
    public void testCreate() {
        TransportOptions options = new TransportOptions();

        assertEquals(TransportOptions.DEFAULT_TCP_NO_DELAY, options.isTcpNoDelay());
    }

    @Test
    public void testOptions() {
        TransportOptions options = createNonDefaultOptions();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(TEST_DEFAULT_TCP_PORT, options.getDefaultTcpPort());
    }

    @Test
    public void testClone() {
        TransportOptions options = createNonDefaultOptions().clone();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(TEST_DEFAULT_TCP_PORT, options.getDefaultTcpPort());
    }

    @Test
    public void testSendBufferSizeValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.setSendBufferSize(0);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.setSendBufferSize(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.setSendBufferSize(1);
    }

    @Test
    public void testReceiveBufferSizeValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.setReceiveBufferSize(0);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.setReceiveBufferSize(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.setReceiveBufferSize(1);
    }

    @Test
    public void testTrafficClassValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.setTrafficClass(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.setTrafficClass(256);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.setTrafficClass(0);
        options.setTrafficClass(128);
        options.setTrafficClass(255);
    }

    private TransportOptions createNonDefaultOptions() {
        TransportOptions options = new TransportOptions();

        options.setSendBufferSize(TEST_SEND_BUFFER_SIZE);
        options.setReceiveBufferSize(TEST_RECEIVE_BUFFER_SIZE);
        options.setTrafficClass(TEST_TRAFFIC_CLASS);
        options.setTcpNoDelay(TEST_TCP_NO_DELAY);
        options.setTcpKeepAlive(TEST_TCP_KEEP_ALIVE);
        options.setSoLinger(TEST_SO_LINGER);
        options.setSoTimeout(TEST_SO_TIMEOUT);
        options.setConnectTimeout(TEST_CONNECT_TIMEOUT);
        options.setDefaultTcpPort(TEST_DEFAULT_TCP_PORT);

        return options;
    }
}
