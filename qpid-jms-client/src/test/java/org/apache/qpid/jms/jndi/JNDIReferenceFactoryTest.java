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
package org.apache.qpid.jms.jndi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Before;
import org.junit.Test;

public class JNDIReferenceFactoryTest extends QpidJmsTestCase {

    private static final String DESTINATION_NAME_PROP = "address";
    private static final String LEGACY_DESTINATION_NAME_PROP = "name";
    private static final String REMOTE_URI_NAME_PROP = "remoteURI";

    private static final String TEST_CONNECTION_URL = "amqp://somehost:2765";
    private static final String TEST_QUEUE_ADDRESS = "myQueue";
    private static final String TEST_TOPIC_ADDRESS = "myTopic";

    private Name mockName;
    private Context mockContext;
    private Hashtable<?, ?> testEnvironment;
    private ObjectFactory referenceFactory;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        mockName = mock(Name.class);
        mockContext = mock(Context.class);
        testEnvironment = new Hashtable<>();

        referenceFactory = new JNDIReferenceFactory();
    }

    @Test
    public void testGetObjectInstanceCreatesJmsConnectionFactory() throws Exception {
        Reference reference = createTestReference(JmsConnectionFactory.class.getName(), REMOTE_URI_NAME_PROP, TEST_CONNECTION_URL);

        Object connFactory = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment);

        assertNotNull("Expected object to be created", connFactory);
        assertEquals("Unexpected object type created", JmsConnectionFactory.class, connFactory.getClass());
        assertEquals("Unexpected URI", TEST_CONNECTION_URL, ((JmsConnectionFactory) connFactory).getRemoteURI());
    }

    @Test
    public void testGetObjectInstanceCreatesJmsQueue() throws Exception {
        doGetObjectInstanceCreatesJmsQueueTestImpl(DESTINATION_NAME_PROP);
    }

    @Test
    public void testGetObjectInstanceCreatesJmsQueueUsingLegacyNameProp() throws Exception {
        doGetObjectInstanceCreatesJmsQueueTestImpl(LEGACY_DESTINATION_NAME_PROP);
    }

    private void doGetObjectInstanceCreatesJmsQueueTestImpl(String nameAddressProp) throws Exception, JMSException {
        Reference reference = createTestReference(JmsQueue.class.getName(), nameAddressProp, TEST_QUEUE_ADDRESS);

        Object queue = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment);

        assertNotNull("Expected object to be created", queue);
        assertEquals("Unexpected object type created", JmsQueue.class, queue.getClass());
        assertEquals("Unexpected address", TEST_QUEUE_ADDRESS, ((JmsQueue) queue).getAddress());
        assertEquals("Unexpected queue name", TEST_QUEUE_ADDRESS, ((Queue) queue).getQueueName());

    }

    @Test
    public void testGetObjectInstanceCreatesJmsTopic() throws Exception {
        doGetObjectInstanceCreatesJmsTopicTestImpl(DESTINATION_NAME_PROP);
    }

    @Test
    public void testGetObjectInstanceCreatesJmsTopicUsingLegacyNameProp() throws Exception {
        doGetObjectInstanceCreatesJmsTopicTestImpl(LEGACY_DESTINATION_NAME_PROP);
    }

    private void doGetObjectInstanceCreatesJmsTopicTestImpl(String nameAddressProp) throws Exception, JMSException {
        Reference reference = createTestReference(JmsTopic.class.getName(), nameAddressProp, TEST_TOPIC_ADDRESS);

        Object topic = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment);

        assertNotNull("Expected object to be created", topic);
        assertEquals("Unexpected object type created", JmsTopic.class, topic.getClass());
        assertEquals("Unexpected address", TEST_TOPIC_ADDRESS, ((JmsTopic) topic).getAddress());
        assertEquals("Unexpected queue name", TEST_TOPIC_ADDRESS, ((Topic) topic).getTopicName());
    }

    private Reference createTestReference(String className, String addressType, Object content) {
        Reference mockReference = mock(Reference.class);
        when(mockReference.getClassName()).thenReturn(className);

        RefAddr mockRefAddr = mock(StringRefAddr.class);
        when(mockRefAddr.getType()).thenReturn(addressType);
        when(mockRefAddr.getContent()).thenReturn(content);

        RefAddrTestEnumeration testEnumeration = new RefAddrTestEnumeration(mockRefAddr);
        when(mockReference.getAll()).thenReturn(testEnumeration);

        return mockReference;
    }

    private class RefAddrTestEnumeration implements Enumeration<RefAddr> {
        boolean hasMore = true;
        final RefAddr element;

        public RefAddrTestEnumeration(RefAddr mockAddr) {
            element = mockAddr;
        }

        @Override
        public boolean hasMoreElements() {
            return hasMore;
        }

        @Override
        public RefAddr nextElement() {
            if (!hasMore) {
                throw new NoSuchElementException("No more elements");
            }

            hasMore = false;
            return element;
        }
    }

    @Test
    public void testGetObjectInstanceWithUnknownClassName() throws Exception {
        Reference reference = createTestReference(Object.class.getName(), "redundant", "redundant");

        Object factory = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment);
        assertNull("Expected null when given unknown class name", factory);
    }
}
