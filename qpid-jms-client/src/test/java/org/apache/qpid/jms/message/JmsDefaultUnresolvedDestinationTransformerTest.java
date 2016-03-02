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
package org.apache.qpid.jms.message;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.junit.Test;
import org.mockito.Mockito;

public class JmsDefaultUnresolvedDestinationTransformerTest {

    private final JmsDefaultUnresolvedDestinationTransformer transformer = new JmsDefaultUnresolvedDestinationTransformer();

    @Test
    public void testTransformDestinationDestinationWithNoNameThrowsJMSEx() throws JMSException {
        Destination destination = Mockito.mock(Destination.class);
        try {
            transformer.transform(destination);
            fail("Should throw a JMSException here");
        } catch (JMSException ex) {
        }
    }

    @Test
    public void testTransformDestinationDestinationWithoutTypeThrowsJMSEx() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue("destination");
        destination.setTopic(false);
        destination.setQueue(false);

        try {
            transformer.transform(destination);
            fail("Should throw a JMSException here");
        } catch (JMSException ex) {
        }
    }

    @Test
    public void testTopicTypeIsDetectedFromComposite() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue("destination");
        destination.setQueue(false);

        JmsDestination result = transformer.transform(destination);

        assertNotNull(result);
        assertTrue(result instanceof JmsTopic);
    }

    @Test
    public void testQueueTypeIsDetectedFromComposite() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue("destination");
        destination.setTopic(false);

        JmsDestination result = transformer.transform(destination);

        assertNotNull(result);
        assertTrue(result instanceof JmsQueue);
    }

    @Test
    public void testTopicTypeIsDetectedFromCompositeMisMatchedNameAndType() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue("destination");
        destination.setQueue(false);
        destination.setReturnTopicName(false);

        JmsDestination result = transformer.transform(destination);

        assertNotNull(result);
        assertTrue(result instanceof JmsTopic);
    }

    @Test
    public void testQueueTypeIsDetectedFromCompositeMisMatchedNameAndType() throws JMSException {
        ForeignTopicAndQueue destination = new ForeignTopicAndQueue("destination");
        destination.setTopic(false);
        destination.setReturnQueueName(false);

        JmsDestination result = transformer.transform(destination);

        assertNotNull(result);
        assertTrue(result instanceof JmsQueue);
    }

    @Test
    public void testTransformStringAlwaysAnswersQueue() throws JMSException {
        JmsDestination destination = transformer.transform("test");
        assertNotNull(destination);
        assertTrue(destination instanceof JmsQueue);
    }

    @Test(expected=JMSException.class)
    public void testTransformStringWithNullThrowsJMSEx() throws JMSException {
        transformer.transform((String) null);
    }

    //-------- Custom foreign destination type -------------------------------//

    @SuppressWarnings("unused")
    private class ForeignTopicAndQueue implements Queue, Topic {

        private boolean returnTopicName = true;
        private boolean returnQueueName = true;

        private boolean topic = true;
        private boolean queue = true;

        private final String name;

        public ForeignTopicAndQueue(String name) {
            this.name = name;
        }

        @Override
        public String getTopicName() throws JMSException {
            if (returnTopicName) {
                return name;
            }

            return null;
        }

        @Override
        public String getQueueName() throws JMSException {
            if (returnQueueName) {
                return name;
            }

            return null;
        }

        public boolean isTopic() {
            return topic;
        }

        public void setTopic(boolean value) {
            this.topic = value;
        }

        public boolean isQueue() {
            return queue;
        }

        public void setQueue(boolean value) {
            this.queue = value;
        }

        public void setReturnTopicName(boolean returnTopicName) {
            this.returnTopicName = returnTopicName;
        }

        public void setReturnQueueName(boolean returnQueueName) {
            this.returnQueueName = returnQueueName;
        }
    }
}
