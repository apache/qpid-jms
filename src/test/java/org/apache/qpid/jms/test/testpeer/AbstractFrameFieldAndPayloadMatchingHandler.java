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
package org.apache.qpid.jms.test.testpeer;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.hamcrest.Matcher;

public abstract class AbstractFrameFieldAndPayloadMatchingHandler extends FrameMatchingHandler
{
    private final Map<Integer, Matcher<?>> _matchers;
    private Map<Integer, Object> _receivedFields;

    protected AbstractFrameFieldAndPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor,
                                                Map<Integer, Matcher<?>> matchers,
                                                Runnable onSuccess)
    {
        super(frameType, channel, numericDescriptor, symbolicDescriptor, onSuccess);
        _matchers = matchers;
    }

    protected Map<Integer, Matcher<?>> getMatchers()
    {
        return _matchers;
    }

    @Override
    protected Map<Integer, Object> getReceivedFields()
    {
        return _receivedFields;
    }

    @Override
    protected void frame(List<Object> described, Binary payload)
    {
        verifyPayload(payload);
        verifyFields(described);

        succeeded();
    }

    protected void verifyFields(List<Object> described)
    {
        int i = 0;
        HashMap<Integer, Object> valueMap = new HashMap<>();
        for(Object value : described)
        {
            valueMap.put(i++, value);
        }

        _receivedFields = valueMap;

        for(Map.Entry<Integer, Matcher<?>> entry : _matchers.entrySet())
        {
            @SuppressWarnings("unchecked")
            Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
            Integer field = entry.getKey();

            assertThat("Field value should match", valueMap.get(field), matcher);
        }
    }

    protected abstract void verifyPayload(Binary payload);
}