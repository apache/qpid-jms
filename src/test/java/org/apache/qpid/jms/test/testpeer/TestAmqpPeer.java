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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.AttachFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.BeginFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.CloseFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.DispositionFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.FlowFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.OpenFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslMechanismsFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslOutcomeFrame;
import org.apache.qpid.jms.test.testpeer.matchers.AttachMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.BeginMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.CloseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.OpenMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SaslInitMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransferMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;


public class TestAmqpPeer implements AutoCloseable
{
    /** Roles are represented as booleans - see AMQP spec 2.8.1*/
    private static final boolean SENDER_ROLE = false;

    /** Roles are represented as booleans - see AMQP spec 2.8.1*/
    private static final boolean RECEIVER_ROLE = true;

    private static final UnsignedByte ATTACH_SND_SETTLE_MODE_UNSETTLED = UnsignedByte.valueOf((byte) 0);

    private static final UnsignedByte ATTACH_RCV_SETTLE_MODE_FIRST = UnsignedByte.valueOf((byte)0);

    private final TestAmqpPeerRunner _driverRunnable;
    private final Thread _driverThread;
    private final List<Handler> _handlers = Collections.synchronizedList(new ArrayList<Handler>());

    public TestAmqpPeer(int port) throws IOException
    {
        _driverRunnable = new TestAmqpPeerRunner(port, this);
        _driverThread = new Thread(_driverRunnable, "MockAmqpPeerThread");
        _driverThread.start();
    }

    @Override
    public void close() throws IOException
    {
        _driverRunnable.stop();

        try
        {
            _driverThread.join(30000);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    public Exception getException()
    {
        return _driverRunnable.getException();
    }

    public void receiveHeader(byte[] header)
    {
        Handler handler = getFirstHandler();
        if(handler instanceof HeaderHandler)
        {
            ((HeaderHandler)handler).header(header,this);
            if(handler.isComplete())
            {
                _handlers.remove(0);
            }
        }
        else
        {
            throw new IllegalStateException("Received header but the next handler is a " + handler);
        }
    }

    public void receiveFrame(int type, int channel, DescribedType describedType, Binary payload)
    {
        Handler handler = getFirstHandler();
        if(handler instanceof FrameHandler)
        {
            ((FrameHandler)handler).frame(type, channel, describedType, payload, this);
            if(handler.isComplete())
            {
                _handlers.remove(0);
            }
        }
        else
        {
            throw new IllegalStateException("Received frame but the next handler is a " + handler);
        }
    }

    private Handler getFirstHandler()
    {
        if(_handlers.isEmpty())
        {
            throw new IllegalStateException("No handlers");
        }
        return _handlers.get(0);
    }

    void sendHeader(byte[] header)
    {
        _driverRunnable.sendBytes(header);
    }

    public void sendFrame(FrameType type, int channel, DescribedType describedType, Binary payload)
    {
        byte[] output = AmqpDataFramer.encodeFrame(type, channel, describedType, payload);
        _driverRunnable.sendBytes(output);
    }

    public void expectPlainConnect(String username, String password, boolean authorize)
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(Symbol.valueOf("PLAIN"));
        _handlers.add(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        byte[] usernameBytes = username.getBytes();
        byte[] passwordBytes = password.getBytes();
        byte[] data = new byte[usernameBytes.length+passwordBytes.length+2];
        System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);

        _handlers.add(new SaslInitMatcher()
            .withMechanism(equalTo(Symbol.valueOf("PLAIN")))
            .withInitialResponse(equalTo(new Binary(data)))
            .onSuccess(new AmqpPeerRunnable()
            {
                @Override
                public void run()
                {
                    TestAmqpPeer.this.sendFrame(
                            FrameType.SASL, 0,
                            new SaslOutcomeFrame().setCode(UnsignedByte.valueOf((byte)0)),
                            null);
                    _driverRunnable.expectHeader();
                }
            }));

        _handlers.add(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));

        _handlers.add(new OpenMatcher()
            .withContainerId(notNullValue(String.class))
            .onSuccess(new FrameSender(
                    this, FrameType.AMQP, 0,
                    new OpenFrame().setContainerId("test-amqp-peer-container-id"),
                    null)));
    }

    public void expectClose()
    {
        _handlers.add(new CloseMatcher()
            .withError(Matchers.nullValue())
            .onSuccess(new FrameSender(this, FrameType.AMQP, 0,
                    new CloseFrame(),
                    null)));
    }

    public void expectBegin()
    {
        final BeginMatcher beginMatcher = new BeginMatcher()
                .withRemoteChannel(nullValue())
                .withNextOutgoingId(notNullValue())
                .withIncomingWindow(notNullValue())
                .withOutgoingWindow(notNullValue());

        // the response will have its remote channel dynamically set based on incoming value
        final BeginFrame beginResponse = new BeginFrame()
            .setNextOutgoingId(UnsignedInteger.valueOf(0))
            .setIncomingWindow(UnsignedInteger.valueOf(0))
            .setOutgoingWindow(UnsignedInteger.valueOf(0));

        beginMatcher.onSuccess(
                new FrameSender(this, FrameType.AMQP, 0, beginResponse, null)
                    .setValueProvider(new ValueProvider()
                        {
                            @Override
                            public void setValues()
                            {
                                beginResponse.setRemoteChannel(
                                        UnsignedShort.valueOf((short) beginMatcher.getActualChannel()));
                            }
                        }));

        _handlers.add(beginMatcher);
    }

    public void expectSenderAttach()
    {
        expectAttach(SENDER_ROLE);
    }

    private void expectAttach(boolean role)
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(role))
                .withSndSettleMode(equalTo(ATTACH_SND_SETTLE_MODE_UNSETTLED))
                .withRcvSettleMode(equalTo(ATTACH_RCV_SETTLE_MODE_FIRST))
                .withSource(notNullValue())
                .withTarget(notNullValue());

        UnsignedInteger linkHandle = UnsignedInteger.valueOf(101);
        final AttachFrame attachResponse = new AttachFrame()
                            .setHandle(linkHandle)
                            .setRole(!role)
                            .setSndSettleMode(ATTACH_SND_SETTLE_MODE_UNSETTLED)
                            .setRcvSettleMode(ATTACH_RCV_SETTLE_MODE_FIRST);

        FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, 0, attachResponse, null)
                            .setValueProvider(new ValueProvider()
                            {
                                @Override
                                public void setValues()
                                {
                                    attachResponse.setName(attachMatcher.getReceivedName());
                                    attachResponse.setSource(attachMatcher.getReceivedSource());
                                    attachResponse.setTarget(attachMatcher.getReceivedTarget());
                                }
                            });

        final FlowFrame flowFrame = new FlowFrame().setNextIncomingId(UnsignedInteger.valueOf(0))
                .setIncomingWindow(UnsignedInteger.valueOf(2048))
                .setNextOutgoingId(UnsignedInteger.valueOf(0))
                .setOutgoingWindow(UnsignedInteger.valueOf(2048))
                .setLinkCredit(UnsignedInteger.valueOf(100))
                .setHandle(linkHandle);

        FrameSender flowFrameSender = new FrameSender(this, FrameType.AMQP, 0, flowFrame, null)
                            .setValueProvider(new ValueProvider()
                            {
                                @Override
                                public void setValues()
                                {
                                    flowFrame.setDeliveryCount(attachMatcher.getReceivedInitialDeliveryCount());
                                }
                            });

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(attachResponseSender);
        composite.add(flowFrameSender);

        attachMatcher.onSuccess(composite);

        _handlers.add(attachMatcher);
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher)
    {
        final TransferMatcher transferMatcher = new TransferMatcher();
        transferMatcher.setPayloadMatcher(expectedPayloadMatcher);

        final DispositionFrame dispositionFrame = new DispositionFrame()
                                                   .setRole(RECEIVER_ROLE)
                                                   .setSettled(true)
                                                   .setState(new Accepted());

        FrameSender dispositionFrameSender = new FrameSender(this, FrameType.AMQP, 0, dispositionFrame, null)
                            .setValueProvider(new ValueProvider()
                            {
                                @Override
                                public void setValues()
                                {
                                    dispositionFrame.setFirst(transferMatcher.getReceivedDeliveryId());
                                }
                            });
        transferMatcher.onSuccess(dispositionFrameSender);

        _handlers.add(transferMatcher);
    }
}
