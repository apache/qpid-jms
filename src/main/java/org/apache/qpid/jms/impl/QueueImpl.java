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
package org.apache.qpid.jms.impl;

import javax.jms.JMSException;
import javax.jms.Queue;

class QueueImpl implements Queue
{
    private final String _queueName;

    public QueueImpl(String queueName)
    {
        if(queueName == null)
        {
            throw new IllegalArgumentException("Queue name must be specified");
        }

        _queueName = queueName;
    }

    @Override
    public String getQueueName() throws JMSException
    {
        return _queueName;
    }

    @Override
    public String toString()
    {
        return _queueName;
    }

    @Override
    public int hashCode()
    {
        return _queueName.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        return _queueName.equals(((QueueImpl)o)._queueName);
    }
}
