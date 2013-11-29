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

import java.util.HashSet;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

public class DestinationHelper
{
    public static final String TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME = "x-opt-to-type";

    static final String QUEUE_ATTRIBUTE = "queue";
    static final String TOPIC_ATTRIBUTE = "topic";
    static final String TEMPORARY_ATTRIBUTE = "temporary";

    public static final String QUEUE_ATTRIBUTES_STRING = QUEUE_ATTRIBUTE;
    public static final String TOPIC_ATTRIBUTES_STRING = TOPIC_ATTRIBUTE;
    public static final String TEMP_QUEUE_ATTRIBUTES_STRING = QUEUE_ATTRIBUTE + "," + TEMPORARY_ATTRIBUTE;
    public static final String TEMP_TOPIC_ATTRIBUTES_STRING = TOPIC_ATTRIBUTE + "," + TEMPORARY_ATTRIBUTE;

    public DestinationHelper()
    {
    }

    public Queue createQueue(String address)
    {
        return new QueueImpl(address);
    }

    public Topic createTopic(String address)
    {
        return new TopicImpl(address);
    }

    public Destination decodeDestination(String address, String typeString)
    {
        Set<String> typeSet = null;

        if(typeString != null)
        {
            typeSet = splitAttributes(typeString);
        }

        return createDestination(address, typeSet);
    }

    private Destination createDestination(String address, Set<String> typeSet)
    {
        if(address == null)
        {
            return null;
        }

        if(typeSet == null || typeSet.isEmpty())
        {
            //TODO: characterise Destination used to create the receiver, and create that type
        }
        else
        {
            if(typeSet.contains(QUEUE_ATTRIBUTE))
            {
                if(typeSet.contains(TEMPORARY_ATTRIBUTE))
                {
                    //TODO
                    throw new IllegalArgumentException("TemporaryQueue not yet supported");
                }
                else
                {
                    return createQueue(address);
                }
            }
            else if(typeSet.contains(TOPIC_ATTRIBUTE))
            {
                if(typeSet.contains(TEMPORARY_ATTRIBUTE))
                {
                    //TODO
                    throw new IllegalArgumentException("TemporaryTopic not yet supported");
                }
                else
                {
                    return createTopic(address);
                }
            }
        }

        //fall back to a straight Destination
        return new DestinationImpl(address);
    }

    public Destination convertToQpidDestination(Destination dest) throws JMSException
    {
        if(dest == null)
        {
            return null;
        }

        if(isQpidDestination(dest))
        {
            return dest;
        }
        else
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
                return createQueue(((Queue) dest).getQueueName());
            }
            else if(dest instanceof Topic)
            {
                return createTopic(((Topic) dest).getTopicName());
            }
            else
            {
                throw new IllegalArgumentException("Unsupported Destination type: " + dest.getClass().getName());
            }
        }
    }

    public boolean isQpidDestination(Destination dest)
    {
        return dest instanceof DestinationImpl;
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

        if(destination instanceof Queue)
        {
            return ((Queue) destination).getQueueName();
        }
        else if(destination instanceof Topic)
        {
            return ((Topic) destination).getTopicName();
        }
        else
        {
            throw new IllegalArgumentException("Support for those destinations not yet implemented");
        }
    }

    /**
     * @return the annotation type string, or null if the supplied destination is null or can't be classified
     */
    public String decodeTypeString(Destination destination)
    {
        if(destination == null)
        {
            return null;
        }

        if(destination instanceof TemporaryQueue)
        {
            return TEMP_QUEUE_ATTRIBUTES_STRING;
        }
        else if(destination instanceof Queue)
        {
            return QUEUE_ATTRIBUTES_STRING;
        }
        else if(destination instanceof TemporaryTopic)
        {
            return TEMP_TOPIC_ATTRIBUTES_STRING;
        }
        else if(destination instanceof Topic)
        {
            return TOPIC_ATTRIBUTES_STRING;
        }
        else
        {
            //unable to classify
            return null;
        }
    }

    Set<String> splitAttributes(String typeString)
    {
        if( typeString == null )
        {
            return null;
        }

        HashSet<String> typeSet = new HashSet<String>();

        //Split string on commas and their surrounding whitespace
        for( String attr : typeString.split("\\s*,\\s*") )
        {
            //ignore empty values
            if(!attr.equals(""))
            {
                typeSet.add(attr);
            }
        }

        return typeSet;
    }
}
