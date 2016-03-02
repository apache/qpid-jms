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

import java.lang.reflect.Method;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;

/**
 * Default Destination resolver that will try and find a way to convert an unknown foreign
 * JMS Destination object by looking for method in the object to identify the true type.
 *
 * For a String destination this class will always return a Queue.
 */
public class JmsDefaultUnresolvedDestinationTransformer implements JmsUnresolvedDestinationTransformer {

    @Override
    public JmsDestination transform(Destination destination) throws JMSException {

        String queueName = null;
        String topicName = null;

        if (destination instanceof Queue) {
            queueName = ((Queue) destination).getQueueName();
        }

        if (destination instanceof Topic) {
            topicName = ((Topic) destination).getTopicName();
        }

        if (queueName == null && topicName == null) {
            throw new JMSException("Unresolvable destination: Both queue and topic names are null: " + destination);
        }

        try {
            Method isQueueMethod = destination.getClass().getMethod("isQueue");
            Method isTopicMethod = destination.getClass().getMethod("isTopic");
            Boolean isQueue = (Boolean) isQueueMethod.invoke(destination);
            Boolean isTopic = (Boolean) isTopicMethod.invoke(destination);
            if (isQueue) {
                return new JmsQueue(queueName);
            } else if (isTopic) {
                return new JmsTopic(topicName);
            } else {
                throw new JMSException("Unresolvable destination: Neither Queue nor Topic: " + destination);
            }
        } catch (Exception e)  {
            throw new JMSException("Unresolvable destination: "  + e.getMessage() + ": " + destination);
        }
    }

    @Override
    public JmsDestination transform(String destination) throws JMSException {
        if (destination == null) {
            throw new JMSException("Destination objects cannot have a null name value");
        }

        return new JmsQueue(destination);
    }
}
