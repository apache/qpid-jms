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

public class FrameSender implements AmqpPeerRunnable
{
    private final TestAmqpPeer _testAmqpPeer;
    private final FrameType _type;
    private final ListDescribedType _frameDescribedType;
    private final Binary _framePayload;
    private ValueProvider _valueProvider;
    private int _channel;
    private boolean _deferWrite = false;
    private long _sendDelay = 0;

    FrameSender(TestAmqpPeer testAmqpPeer, FrameType type, int channel, ListDescribedType frameDescribedType, Binary framePayload)
    {
        _testAmqpPeer = testAmqpPeer;
        _type = type;
        _channel = channel;
        _frameDescribedType = frameDescribedType;
        _framePayload = framePayload;
    }

    @Override
    public void run()
    {
        if(_valueProvider != null)
        {
            _valueProvider.setValues();
        }

        _testAmqpPeer.sendFrame(_type, _channel, _frameDescribedType, _framePayload, _deferWrite, _sendDelay);
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

    public FrameSender setDeferWrite(boolean deferWrite)
    {
        _deferWrite  = deferWrite;
        return this;
    }

    public void setSendDelay(long _sendDelay) {
        this._sendDelay = _sendDelay;
    }
}