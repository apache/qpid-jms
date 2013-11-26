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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AmqpMessageTest extends QpidJmsTestCase
{
    private static final String TEST_PROP_A = "TEST_PROP_A";
    private static final String TEST_PROP_B = "TEST_PROP_B";
    private static final String TEST_VALUE_STRING_A = "TEST_VALUE_STRING_A";
    private static final String TEST_VALUE_STRING_B = "TEST_VALUE_STRING_B";

    private AmqpConnection _mockAmqpConnection;
    private Delivery _mockDelivery;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _mockAmqpConnection = Mockito.mock(AmqpConnection.class);
        _mockDelivery = Mockito.mock(Delivery.class);
    }

    @Test
    public void testGetApplicationPropertyNames()
    {
        //Check a Proton Message without any application properties section
        Message message1 = Proton.message();
        TestAmqpMessage testAmqpMessage1 = new TestAmqpMessage(message1, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames = testAmqpMessage1.getApplicationPropertyNames();
        assertNotNull(applicationPropertyNames);
        assertTrue(applicationPropertyNames.isEmpty());

        //Check a Proton Message with some application properties
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);
        applicationPropertiesMap.put(TEST_PROP_B, TEST_VALUE_STRING_B);

        Message message2 = Proton.message();
        message2.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        TestAmqpMessage testAmqpMessage2 = new TestAmqpMessage(message2, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames2 = testAmqpMessage2.getApplicationPropertyNames();
        assertEquals(2, applicationPropertyNames2.size());
        assertTrue(applicationPropertyNames2.contains(TEST_PROP_A));
        assertTrue(applicationPropertyNames2.contains(TEST_PROP_B));
    }

    @Test
    public void testClearAllApplicationProperties()
    {
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        Set<String> applicationPropertyNames = testAmqpMessage.getApplicationPropertyNames();
        assertEquals(1, applicationPropertyNames.size());
        assertTrue(applicationPropertyNames.contains(TEST_PROP_A));

        //Now empty the application properties
        testAmqpMessage.clearAllApplicationProperties();
        applicationPropertyNames = testAmqpMessage.getApplicationPropertyNames();
        assertTrue(applicationPropertyNames.isEmpty());
    }

    @Test
    public void testApplicationPropertyExists()
    {
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertTrue(testAmqpMessage.applicationPropertyExists(TEST_PROP_A));
        assertFalse(testAmqpMessage.applicationPropertyExists(TEST_PROP_B));
    }

    @Test
    public void testGetApplicationProperty()
    {
        Map<Object,Object> applicationPropertiesMap = new HashMap<Object,Object>();
        applicationPropertiesMap.put(TEST_PROP_A, TEST_VALUE_STRING_A);

        Message message = Proton.message();
        message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap ));

        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertEquals(TEST_VALUE_STRING_A, testAmqpMessage.getApplicationProperty(TEST_PROP_A));
        assertNull(testAmqpMessage.getApplicationProperty(TEST_PROP_B));
    }

    @Test
    public void testSetApplicationProperty()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        assertNull(testAmqpMessage.getApplicationProperty(TEST_PROP_A));
        testAmqpMessage.setApplicationProperty(TEST_PROP_A, TEST_VALUE_STRING_A);
        assertEquals(TEST_VALUE_STRING_A, testAmqpMessage.getApplicationProperty(TEST_PROP_A));
    }

    @Test
    public void testSetApplicationPropertyUsingNullKeyCausesIAE()
    {
        Message message = Proton.message();
        TestAmqpMessage testAmqpMessage = new TestAmqpMessage(message, _mockDelivery, _mockAmqpConnection);

        try
        {
            testAmqpMessage.setApplicationProperty(null, "value");
            fail("expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

}
