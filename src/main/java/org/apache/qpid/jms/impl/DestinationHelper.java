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

import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

public class DestinationHelper
{
    //TODO: this only deals with Queues currently as that is all that is implemented so far. This will
    //eventually need to distinguish Queues, Topics, and possibly 'Destinations' that are neither.
    public DestinationHelper()
    {
    }

    public Destination decodeDestination(String address, String typeString)
    {
        Set<String> typeSet = null;

        if(typeString != null)
        {
            //TODO
            throw new IllegalArgumentException("Support for type classification not yet present");
        }

        return createDestination(address, typeSet);
    }

    private Destination createDestination(String address, Set<String> typeSet)
    {
        if(address == null)
        {
            return null;
        }

        if(typeSet != null)
        {
            //TODO
            throw new IllegalArgumentException("Support for type classification not yet present");
        }

        return new QueueImpl(address);
    }

    public Destination convertToQpidDestination(Destination dest) throws JMSException
    {
        if(dest == null)
        {
            return null;
        }

        if(!(isQpidDestination(dest)))
        {
            if(dest instanceof TemporaryQueue)
            {
                //TODO
                throw new IllegalArgumentException("Unsupported Destination type: " + dest.getClass().getName());
            }
            else if(dest instanceof TemporaryTopic)
            {
                //TODO
                throw new IllegalArgumentException("Unsupported Destination type: " + dest.getClass().getName());
            }
            else if(dest instanceof Queue)
            {
                return createDestination(((Queue) dest).getQueueName(), null);
            }
            else if(dest instanceof Topic)
            {
                //TODO
                throw new IllegalArgumentException("Unsupported Destination type: " + dest.getClass().getName());
            }
            else
            {
                throw new IllegalArgumentException("Unsupported Destination type: " + dest.getClass().getName());
            }
        }
        else
        {
            return dest;
        }
    }

    public boolean isQpidDestination(Destination dest)
    {
        //TODO: support other destination types when implemented
        return dest instanceof QueueImpl;
    }

    public String decodeAddress(Destination destination) throws JMSException
    {
        if(destination == null)
        {
            return null;
        }

        if(!isQpidDestination(destination))
        {
            destination = convertToQpidDestination(destination);
        }

        if(destination instanceof QueueImpl)
        {
            return ((QueueImpl) destination).getQueueName();
        }
        throw new IllegalArgumentException("Support for those destinations not yet implemented");
    }
}
