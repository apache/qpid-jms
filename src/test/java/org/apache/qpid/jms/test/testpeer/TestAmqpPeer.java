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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.jms.test.testpeer.frames.CloseFrame;
import org.apache.qpid.jms.test.testpeer.frames.OpenFrame;
import org.apache.qpid.jms.test.testpeer.frames.SaslMechanismsFrame;
import org.apache.qpid.jms.test.testpeer.frames.SaslOutcomeFrame;
import org.apache.qpid.jms.test.testpeer.matchers.CloseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.OpenMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SaslInitMatcher;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.hamcrest.Matchers;


public class TestAmqpPeer implements AutoCloseable
{
    private final TestAmqpPeerRunner _driverRunnable;
    private final Thread _driverThread;
    private final List<Handler> _handlers = new ArrayList<>();

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
}
