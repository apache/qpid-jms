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

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

/**
 * Temporary Queue Object
 */
public class JmsTemporaryQueue extends JmsDestination implements TemporaryQueue {

    public JmsTemporaryQueue() {
        this(null);
    }

    public JmsTemporaryQueue(String name) {
        super(name, false, true);
    }

    @Override
    public JmsTemporaryQueue copy() {
        final JmsTemporaryQueue copy = new JmsTemporaryQueue();
        copy.setProperties(getProperties());
        return copy;
    }

    /**
     * @see javax.jms.TemporaryQueue#delete()
     */
    @Override
    public void delete() {
        try {
            tryDelete();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return name
     * @see javax.jms.Queue#getQueueName()
     */
    @Override
    public String getQueueName() {
        return getName();
    }
}
