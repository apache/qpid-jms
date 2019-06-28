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

import org.apache.qpid.jms.test.testpeer.describedtypes.FrameDescriptorMapping;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFrameFieldAndPayloadMatchingHandler extends AbstractFieldAndDescriptorMatcher implements FrameHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFrameFieldAndPayloadMatchingHandler.class);

    public static int ANY_CHANNEL = -1;

    private final FrameType _frameType;

    /** The expected channel number, or {@link #ANY_CHANNEL} if we don't care */
    private int _expectedChannel;
    private int _actualChannel;

    private AmqpPeerRunnable _onCompletion;

    private int _expectedFrameSize;

    private boolean _optional;

    protected AbstractFrameFieldAndPayloadMatchingHandler(FrameType frameType,
                                                int channel,
                                                int frameSize,
                                                UnsignedLong numericDescriptor,
                                                Symbol symbolicDescriptor)
    {
        super(numericDescriptor, symbolicDescriptor);
        _frameType = frameType;
        _expectedChannel = channel;
        _expectedFrameSize = frameSize;
    }

    protected abstract void verifyPayload(Binary payload) throws AssertionError;

    @SuppressWarnings("unchecked")
    @Override
    public final void frame(int type, int ch, int frameSize, DescribedType dt, Binary payload, TestAmqpPeer peer)
    {
        if(type == _frameType.ordinal()
           && (_expectedChannel == ANY_CHANNEL || _expectedChannel == ch)
           && descriptorMatches(dt.getDescriptor())
           && (dt.getDescribed() instanceof List))
        {
            _actualChannel = ch;

            try
            {
                verifyFields((List<Object>)dt.getDescribed());
            }
            catch(AssertionError ae)
            {
                LOGGER.error("Failure when verifying frame fields", ae);
                peer.assertionFailed(ae);
            }

            try
            {
                verifyPayload(payload);
            }
            catch(AssertionError ae)
            {
                LOGGER.error("Failure when verifying frame payload", ae);
                peer.assertionFailed(ae);
            }

            if(_expectedFrameSize != 0 && _expectedFrameSize != frameSize) {
                throw new IllegalArgumentException(String.format(
                        "Frame size was not as expected. Expected: " +
                        "type=%s, channel=%s, size=%s, descriptor=%s/%s but got: " +
                        "type=%s, channel=%s, size=%s",
                        _frameType.ordinal(), expectedChannelString(), _expectedFrameSize, getNumericDescriptor(), getSymbolicDescriptor(), type, ch, frameSize));
            }

            if(_onCompletion != null)
            {
                _onCompletion.run();
            }
            else
            {
                LOGGER.debug("No onCompletion action, doing nothing.");
            }
        }
        else
        {
            Object actualDescriptor = dt.getDescriptor();
            Object mappedDescriptor = FrameDescriptorMapping.lookupMapping(actualDescriptor);

            throw new IllegalArgumentException(String.format(
                    "Frame was not as expected. Expected: " +
                    "type=%s, channel=%s, descriptor=%s/%s but got: " +
                    "type=%s, channel=%s, descriptor=%s(%s)",
                    _frameType.ordinal(), expectedChannelString(), getNumericDescriptor(), getSymbolicDescriptor(),
                    type, ch, actualDescriptor, mappedDescriptor));
        }
    }

    private String expectedChannelString()
    {
        return _expectedChannel == ANY_CHANNEL ? "<any>" : String.valueOf(_expectedChannel);
    }

    public AmqpPeerRunnable getOnCompletionAction()
    {
        return _onCompletion;
    }

    public AbstractFrameFieldAndPayloadMatchingHandler onCompletion(AmqpPeerRunnable onSuccessAction)
    {
        _onCompletion = onSuccessAction;
        return this;
    }

    public AbstractFrameFieldAndPayloadMatchingHandler onChannel(int channel)
    {
        _expectedChannel = channel;
        return this;
    }

    public int getActualChannel()
    {
        return _actualChannel;
    }

    @Override
    public void setOptional(boolean optional) {
        _optional = optional;
    }

    @Override
    public boolean isOptional() {
        return _optional;
    }

    @Override
    public String toString()
    {
        return "AbstractFrameFieldAndPayloadMatchingHandler [descriptor=" + getSymbolicDescriptor() + "/" + getNumericDescriptor()
                + ", expectedChannel=" + expectedChannelString()
                + (_optional ? ", optional=true]" : "]");
    }
}