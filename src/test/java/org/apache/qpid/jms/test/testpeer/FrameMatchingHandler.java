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

import java.util.List;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

abstract class FrameMatchingHandler implements FrameHandler
{
    public static int ANY_CHANNEL = -1;

    private final UnsignedLong _numericDescriptor;
    private final Symbol _symbolicDescriptor;
    private final FrameType _frameType;
    private int _channel;
    private Runnable _onSuccessAction;

    protected FrameMatchingHandler(FrameType frameType,
                                   int channel,
                                   UnsignedLong numericDescriptor,
                                   Symbol symbolicDescriptor, Runnable onSuccessAction)
    {
        _frameType = frameType;
        _numericDescriptor = numericDescriptor;
        _symbolicDescriptor = symbolicDescriptor;
        _channel = channel;
        _onSuccessAction = onSuccessAction;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void frame(int type, int ch, DescribedType dt, Binary payload, TestAmqpPeer peer)
    {
        if(type == _frameType.ordinal()
           && (_channel == -1 || _channel == ch)
           && (_numericDescriptor.equals(dt.getDescriptor()) || _symbolicDescriptor.equals(dt.getDescriptor()))
           && (dt.getDescribed() instanceof List))
        {
            frame((List<Object>)dt.getDescribed(),payload);
        }
        else
        {
            throw new IllegalArgumentException(String.format(
                    "Frame was not as expected. Expected: " +
                    "type=%s, channel=%s, descriptor=%s/%s but got: " +
                    "type=%s, channel=%s, descriptor=%s",
                    _frameType.ordinal(), _channel, _symbolicDescriptor, _numericDescriptor,
                    type, ch, dt.getDescriptor()));
        }
    }

    protected void succeeded()
    {
        _onSuccessAction.run();
    }

    public Runnable getOnSuccessAction()
    {
        return _onSuccessAction;
    }

    public FrameMatchingHandler onSuccess(Runnable onSuccessAction)
    {
        _onSuccessAction = onSuccessAction;
        return this;
    }


    public FrameMatchingHandler onChannel(int channel)
    {
        _channel = channel;
        return this;
    }

    protected abstract void frame(List<Object> described, Binary payload);
}