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
 *
 */

package org.apache.qpid.jms.test.testpeer.matchers.sections;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.hamcrest.Matcher;

public class MessageAnnotationsSectionMatcher extends MessageMapSectionMatcher
{
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:message-annotations:map");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000072L);

    public MessageAnnotationsSectionMatcher(boolean expectTrailingBytes)
    {
        super(DESCRIPTOR_CODE,
              DESCRIPTOR_SYMBOL,
              new HashMap<Object, Matcher<?>>(),
              expectTrailingBytes);
    }

    @Override
    public MessageAnnotationsSectionMatcher withEntry(Object key, Matcher<?> m)
    {
        validateType(key);

        return (MessageAnnotationsSectionMatcher) super.withEntry(key, m);
    }

    private void validateType(Object key)
    {
        if(!(key instanceof Long || key instanceof Symbol))
        {
            throw new IllegalArgumentException("Message Annotation keys must be of type Symbol or long (reserved)");
        }
    }

    public boolean keyExistsInReceivedAnnotations(Object key)
    {
        validateType(key);

        Map<Object, Object> receivedFields = super.getReceivedFields();

        if(receivedFields != null)
        {
            return receivedFields.containsKey(key);
        }
        else
        {
            return false;
        }
    }
}

