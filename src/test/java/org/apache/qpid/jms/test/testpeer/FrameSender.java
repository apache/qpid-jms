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

import org.apache.qpid.proton.amqp.Binary;

class FrameSender implements AmqpPeerRunnable
{
    private final TestAmqpPeer _testAmqpPeer;
    private final FrameType _type;
    private final ListDescribedType _listDescribedType;
    private final Binary _payload;
    private ValueProvider _valueProvider;
    private int _channel;

    FrameSender(TestAmqpPeer testAmqpPeer, FrameType type, int channel, ListDescribedType listDescribedType, Binary payload)
    {
        _testAmqpPeer = testAmqpPeer;
        _type = type;
        _channel = channel;
        _listDescribedType = listDescribedType;
        _payload = payload;
    }

    @Override
    public void run()
    {
        if(_valueProvider != null)
        {
            _valueProvider.setValues();
        }

        _testAmqpPeer.sendFrame(_type, _channel, _listDescribedType, _payload);
    }

    public FrameSender setValueProvider(ValueProvider valueProvider)
    {
        _valueProvider = valueProvider;
        return this;
    }

    public FrameSender setChannel(int channel)
    {
        _channel = channel;
        return this;
    }
}