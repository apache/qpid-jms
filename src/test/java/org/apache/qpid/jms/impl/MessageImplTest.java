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
package org.apache.qpid.jms.impl;

import static org.junit.Assert.*;

import java.util.Enumeration;

import javax.jms.MessageFormatException;

import org.apache.qpid.jms.engine.TestAmqpMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MessageImplTest
{
    private ConnectionImpl _mockConnectionImpl;
    private SessionImpl _mockSessionImpl;
    private TestMessageImpl _testMessage;

    @Before
    public void setUp() throws Exception
    {
        _mockConnectionImpl = Mockito.mock(ConnectionImpl.class);
        _mockSessionImpl = Mockito.mock(SessionImpl.class);
        _testMessage = new TestMessageImpl(new TestAmqpMessage(), _mockSessionImpl, _mockConnectionImpl);
    }

    @Test
    public void testSetPropertyWithNullOrEmptyNameThrowsIAE() throws Exception
    {
        try
        {
            _testMessage.setObjectProperty(null, "value");
            fail("Expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }

        try
        {
            _testMessage.setObjectProperty("", "value");
            fail("Expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    @Test
    public void testSetObjectPropertyWithIllegalTypeThrowsMFE() throws Exception
    {
        try
        {
            _testMessage.setObjectProperty("myProperty", new Exception());
            fail("Expected exception not thrown");
        }
        catch(MessageFormatException mfe)
        {
            //expected
        }
    }

    @Test
    public void testSetObjectProperty() throws Exception
    {
        String propertyName = "myProperty";

        Object propertyValue = null;
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Boolean.valueOf(false);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Byte.valueOf((byte)1);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Short.valueOf((short)2);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Integer.valueOf(3);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Long.valueOf(4);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Float.valueOf(5.01F);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = Double.valueOf(6.01);
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));

        propertyValue = "string";
        _testMessage.setObjectProperty(propertyName, propertyValue);
        assertEquals(propertyValue, _testMessage.getObjectProperty(propertyName));
    }

    @Test
    public void testPropertyExists() throws Exception
    {
        String propertyName = "myProperty";

        assertFalse(_testMessage.propertyExists(propertyName));
        _testMessage.setObjectProperty(propertyName, "string");
        assertTrue(_testMessage.propertyExists(propertyName));
    }

    @Test
    public void testGetPropertyNames() throws Exception
    {
        String propertyName = "myProperty";

        _testMessage.setObjectProperty(propertyName, "string");
        Enumeration<?> names = _testMessage.getPropertyNames();

        assertTrue(names.hasMoreElements());
        Object name1 = names.nextElement();
        assertTrue(name1 instanceof String);
        assertTrue(propertyName.equals(name1));
        assertFalse(names.hasMoreElements());
    }
}
