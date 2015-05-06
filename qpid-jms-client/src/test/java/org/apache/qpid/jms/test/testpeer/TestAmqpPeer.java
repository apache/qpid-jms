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
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpTemporaryDestination;
import org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper;
import org.apache.qpid.jms.test.testpeer.basictypes.ReceiverSettleMode;
import org.apache.qpid.jms.test.testpeer.basictypes.Role;
import org.apache.qpid.jms.test.testpeer.basictypes.SenderSettleMode;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusDurability;
import org.apache.qpid.jms.test.testpeer.basictypes.TerminusExpiryPolicy;
import org.apache.qpid.jms.test.testpeer.describedtypes.Accepted;
import org.apache.qpid.jms.test.testpeer.describedtypes.AttachFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.BeginFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.CloseFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.Coordinator;
import org.apache.qpid.jms.test.testpeer.describedtypes.DetachFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.DispositionFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.EndFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.FlowFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.OpenFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslMechanismsFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.SaslOutcomeFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.Source;
import org.apache.qpid.jms.test.testpeer.describedtypes.Target;
import org.apache.qpid.jms.test.testpeer.describedtypes.TransferFrame;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.AmqpValueDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.ApplicationPropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.HeaderDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.MessageAnnotationsDescribedType;
import org.apache.qpid.jms.test.testpeer.describedtypes.sections.PropertiesDescribedType;
import org.apache.qpid.jms.test.testpeer.matchers.AttachMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.BeginMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.CloseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DeleteOnCloseMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DetachMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.DispositionMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.EndMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.FlowMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.OpenMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SaslInitMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.SourceMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TargetMatcher;
import org.apache.qpid.jms.test.testpeer.matchers.TransferMatcher;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.codec.Data;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO should expectXXXYYYZZZ methods just be expect(matcher)?
public class TestAmqpPeer implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAmqpPeer.class.getName());
    private static final int CONNECTION_CHANNEL = 0;

    private final TestAmqpPeerRunner _driverRunnable;
    private final Thread _driverThread;

    /**
     * Guarded by {@link #_handlersLock}
     */
    private final List<Handler> _handlers = new ArrayList<Handler>();
    private final Object _handlersLock = new Object();

    /**
     * Guarded by {@link #_handlersLock}
     */
    private CountDownLatch _handlersCompletedLatch;

    private byte[] _deferredBytes;
    private int _lastInitiatedChannel = -1;
    private UnsignedInteger _lastInitiatedLinkHandle = null;

    public TestAmqpPeer() throws IOException
    {
        this(null, false);
    }

    public TestAmqpPeer(SSLContext context, boolean needClientCert) throws IOException
    {
        _driverRunnable = new TestAmqpPeerRunner(this, context, needClientCert);
        _driverThread = new Thread(_driverRunnable, "MockAmqpPeerThread");
        _driverThread.start();
    }

    /**
     * Shuts down the test peer, throwing any Throwable
     * that occurred on the peer, or validating that no
     * unused matchers remain.
     */
    @Override
    public void close() throws Exception
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
        finally
        {
            Throwable throwable = getThrowable();
            if(throwable == null)
            {
                synchronized(_handlersLock)
                {
                    assertThat(_handlers, Matchers.empty());
                }
            }
            else
            {
                //AutoClosable can't handle throwing Throwables, so we wrap it.
                throw new RuntimeException("TestPeer caught throwable during run", throwable);
            }
        }
    }

    public Throwable getThrowable()
    {
        return _driverRunnable.getException();
    }

    public int getServerPort()
    {
        return _driverRunnable.getServerPort();
    }

    public Socket getClientSocket()
    {
        return _driverRunnable.getClientSocket();
    }

    public void receiveHeader(byte[] header)
    {
        Handler handler = getFirstHandler();
        if(handler instanceof HeaderHandler)
        {
            ((HeaderHandler)handler).header(header,this);
            if(handler.isComplete())
            {
                removeFirstHandler();
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
                removeFirstHandler();
            }
        }
        else
        {
            throw new IllegalStateException("Received frame but the next handler is a " + handler);
        }
    }

    private void removeFirstHandler()
    {
        synchronized(_handlersLock)
        {
            Handler h = _handlers.remove(0);
            if(_handlersCompletedLatch != null)
            {
                _handlersCompletedLatch.countDown();
            }
            LOGGER.trace("Removed completed handler: {}", h);
        }
    }

    private void addHandler(Handler handler)
    {
        synchronized(_handlersLock)
        {
            _handlers.add(handler);
            LOGGER.trace("Added handler: {}", handler);
        }
    }

    private Handler getFirstHandler()
    {
        synchronized(_handlersLock)
        {
            if(_handlers.isEmpty())
            {
                throw new IllegalStateException("No handlers");
            }
            return _handlers.get(0);
        }
    }

    private Handler getLastHandler()
    {
        synchronized(_handlersLock)
        {
            if(_handlers.isEmpty())
            {
                throw new IllegalStateException("No handlers");
            }
            return _handlers.get(_handlers.size() - 1);
        }
    }

    public void waitForAllHandlersToComplete(int timeoutMillis) throws InterruptedException
    {
        synchronized(_handlersLock)
        {
            _handlersCompletedLatch = new CountDownLatch(_handlers.size());
        }

        boolean countedDownOk = _handlersCompletedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);

        Assert.assertTrue(
                "All handlers should have completed within the " + timeoutMillis + "ms timeout", countedDownOk);
    }

    void sendHeader(byte[] header)
    {
        LOGGER.debug("About to send header: {}", new Binary(header));
        _driverRunnable.sendBytes(header);
    }

    public void sendFrame(FrameType type, int channel, DescribedType frameDescribedType, Binary framePayload, boolean deferWrite)
    {
        if(channel < 0)
        {
            throw new IllegalArgumentException("Frame must be sent on a channel >= 0");
        }

        LOGGER.debug("About to send: {}", frameDescribedType);
        byte[] output = AmqpDataFramer.encodeFrame(type, channel, frameDescribedType, framePayload);

        if(deferWrite && _deferredBytes == null)
        {
            _deferredBytes = output;
        }
        else if(_deferredBytes != null)
        {
            int newCapacity = _deferredBytes.length + output.length;
            //TODO: check overflow

            byte[] newOutput = new byte[newCapacity];
            System.arraycopy(_deferredBytes, 0, newOutput, 0, _deferredBytes.length);
            System.arraycopy(output, 0, newOutput, _deferredBytes.length, output.length);

            _deferredBytes = newOutput;
            output = newOutput;
        }

        if(deferWrite)
        {
            LOGGER.debug("Deferring write until pipelined with future frame bytes");
            return;
        }
        else
        {
            //clear the deferred bytes to avoid corrupting future sends
            _deferredBytes = null;

            _driverRunnable.sendBytes(output);
        }
    }

    public void expectAnonymousConnect(boolean authorize)
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"));
        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        addHandler(new SaslInitMatcher()
            .withMechanism(equalTo(Symbol.valueOf("ANONYMOUS")))
            .withInitialResponse(nullValue())
            .onSuccess(new AmqpPeerRunnable()
            {
                @Override
                public void run()
                {
                    TestAmqpPeer.this.sendFrame(
                            FrameType.SASL, 0,
                            new SaslOutcomeFrame().setCode(UnsignedByte.valueOf((byte)0)),
                            null,
                            false);
                    _driverRunnable.expectHeader();
                }
            }));

        addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));

        addHandler(new OpenMatcher()
            .withContainerId(notNullValue(String.class))
            .onSuccess(new FrameSender(
                    this, FrameType.AMQP, 0,
                    new OpenFrame().setContainerId("test-amqp-peer-container-id"),
                    null)));
    }

    /**
     * NOTE: this is only testing use of the mechanism, there is no SSL here yet.
     */
    public void expectExternalConnect()
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(Symbol.valueOf("EXTERNAL"));
        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        addHandler(new SaslInitMatcher()
            .withMechanism(equalTo(Symbol.valueOf("EXTERNAL")))
            .withInitialResponse(equalTo(new Binary(new byte[0])))
            .onSuccess(new AmqpPeerRunnable()
            {
                @Override
                public void run()
                {
                    TestAmqpPeer.this.sendFrame(
                            FrameType.SASL, 0,
                            new SaslOutcomeFrame().setCode(UnsignedByte.valueOf((byte)0)),
                            null,
                            false);
                    _driverRunnable.expectHeader();
                }
            }));

        addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));

        addHandler(new OpenMatcher()
            .withContainerId(notNullValue(String.class))
            .onSuccess(new FrameSender(
                    this, FrameType.AMQP, 0,
                    new OpenFrame().setContainerId("test-amqp-peer-container-id"),
                    null)));
    }

    public void expectPlainConnect(String username, String password, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties)
    {
        expectPlainConnect(username, password, new Symbol[] { AmqpConnection.SOLE_CONNECTION_CAPABILITY }, serverCapabilities, serverProperties);
    }

    public void expectPlainConnect(String username, String password, Symbol[] desiredCapabilities, Symbol[] serverCapabilities, Map<Symbol, Object> serverProperties)
    {
        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(Symbol.valueOf("PLAIN"));
        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        byte[] usernameBytes = username.getBytes();
        byte[] passwordBytes = password.getBytes();
        byte[] data = new byte[usernameBytes.length+passwordBytes.length+2];
        System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);

        addHandler(new SaslInitMatcher()
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
                            null,
                            false);
                    _driverRunnable.expectHeader();
                }
            }));

        addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));

        OpenFrame open = new OpenFrame();
        open.setContainerId("test-amqp-peer-container-id");
        if(serverCapabilities != null)
        {
            open.setOfferedCapabilities(serverCapabilities);
        }

        if(serverProperties != null)
        {
            open.setProperties(serverProperties);
        }

        OpenMatcher openMatcher = new OpenMatcher()
            .withContainerId(notNullValue(String.class))
            .onSuccess(new FrameSender(
                    this, FrameType.AMQP, 0,
                    open,
                    null));

        if (desiredCapabilities != null) {
            openMatcher.withDesiredCapabilities(arrayContaining(desiredCapabilities));
        } else {
            openMatcher.withDesiredCapabilities(nullValue());
        }

        addHandler(openMatcher);
    }

    // TODO - Reject any incoming connection using the supplied information
    public void rejectConnect(Symbol errorType, String errorMessage, Map<String, Object> errorInfo) {

        SaslMechanismsFrame saslMechanismsFrame = new SaslMechanismsFrame().setSaslServerMechanisms(Symbol.valueOf("ANONYMOUS"));
        addHandler(new HeaderHandlerImpl(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER,
                                            new FrameSender(
                                                    this, FrameType.SASL, 0,
                                                    saslMechanismsFrame, null)));

        addHandler(new SaslInitMatcher()
            .withMechanism(equalTo(Symbol.valueOf("ANONYMOUS")))
            .withInitialResponse(nullValue())
            .onSuccess(new AmqpPeerRunnable()
            {
                @Override
                public void run()
                {
                    TestAmqpPeer.this.sendFrame(
                            FrameType.SASL, 0,
                            new SaslOutcomeFrame().setCode(UnsignedByte.valueOf((byte)0)),
                            null,
                            false);
                    _driverRunnable.expectHeader();
                }
            }));

        addHandler(new HeaderHandlerImpl(AmqpHeader.HEADER, AmqpHeader.HEADER));

        Map<Symbol, Object> properties = new HashMap<Symbol, Object>();
        properties.put(Symbol.valueOf("amqp:connection-establishment-failed"), true);

        OpenFrame open = new OpenFrame();
        open.setProperties(properties);
        open.setContainerId("test-amqp-peer-container-id");

        addHandler(new OpenMatcher()
            .withContainerId(notNullValue(String.class))
            .onSuccess(new FrameSender(this, FrameType.AMQP, CONNECTION_CHANNEL, open, null)));

        // Now generate the Close with the supplied error
        final CloseFrame closeFrame = new CloseFrame();
        if (errorType != null) {
            org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
            detachError.setCondition(errorType);
            detachError.setDescription(errorMessage);
            detachError.setInfo(errorInfo);
            closeFrame.setError(detachError);
        }

        CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();
        final FrameSender closeSender = new FrameSender(this, FrameType.AMQP, CONNECTION_CHANNEL, closeFrame, null);
        comp.add(closeSender);

        addHandler(new CloseMatcher().withError(Matchers.nullValue()));
    }

    public void expectClose()
    {
        addHandler(new CloseMatcher()
            .withError(Matchers.nullValue())
            .onSuccess(new FrameSender(this, FrameType.AMQP, 0,
                    new CloseFrame(),
                    null)));
    }

    public void expectBegin(boolean expectSessionFlow)
    {
        final BeginMatcher beginMatcher = new BeginMatcher()
                .withRemoteChannel(nullValue())
                .withNextOutgoingId(equalTo(UnsignedInteger.ONE))
                .withIncomingWindow(notNullValue())
                .withOutgoingWindow(notNullValue());

        // The response will have its remoteChannel field dynamically set based on incoming value
        final BeginFrame beginResponse = new BeginFrame()
            .setNextOutgoingId(UnsignedInteger.ONE)
            .setIncomingWindow(UnsignedInteger.ZERO)
            .setOutgoingWindow(UnsignedInteger.ZERO);

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender beginResponseSender = new FrameSender(this, FrameType.AMQP, -1, beginResponse, null);
        beginResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                int actualChannel = beginMatcher.getActualChannel();

                beginResponseSender.setChannel(actualChannel);
                beginResponse.setRemoteChannel(
                        UnsignedShort.valueOf((short) actualChannel));

                _lastInitiatedChannel = actualChannel;
            }
        });
        beginMatcher.onSuccess(beginResponseSender);

        addHandler(beginMatcher);

        if(expectSessionFlow)
        {
            expectSessionFlow();
        }
    }

    public void expectEnd()
    {
        expectEnd(true);
    }

    public void expectEnd(boolean sendResponse)
    {
        final EndMatcher endMatcher = new EndMatcher();

        if (sendResponse) {
            final EndFrame endResponse = new EndFrame();

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, -1, endResponse, null);
            frameSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    frameSender.setChannel(endMatcher.getActualChannel());
                }
            });
            endMatcher.onSuccess(frameSender);
        }

        addHandler(endMatcher);
    }

    public void expectTempQueueCreationAttach(final String dynamicAddress)
    {
        expectTempNodeCreationAttach(dynamicAddress, AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY, false, false, null, null);
    }

    public void expectTempTopicCreationAttach(final String dynamicAddress)
    {
        expectTempNodeCreationAttach(dynamicAddress, AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY, false, false, null, null);
    }

    public void expectAndRefuseTempQueueCreationAttach(Symbol errorType, String errorMessage, boolean deferAttachResponseWrite)
    {
        expectTempNodeCreationAttach(null, AmqpDestinationHelper.TEMP_QUEUE_CAPABILITY, true, deferAttachResponseWrite, errorType, errorMessage);
    }

    public void expectAndRefuseTempTopicCreationAttach(Symbol errorType, String errorMessage, boolean deferAttachResponseWrite)
    {
        expectTempNodeCreationAttach(null, AmqpDestinationHelper.TEMP_TOPIC_CAPABILITY, true, deferAttachResponseWrite, errorType, errorMessage);
    }

    private void expectTempNodeCreationAttach(final String dynamicAddress, final Symbol nodeTypeCapability, final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        TargetMatcher targetMatcher = new TargetMatcher();
        targetMatcher.withAddress(nullValue());
        targetMatcher.withDynamic(equalTo(true));
        targetMatcher.withDurable(equalTo(TerminusDurability.NONE));
        targetMatcher.withExpiryPolicy(equalTo(TerminusExpiryPolicy.LINK_DETACH));
        targetMatcher.withDynamicNodeProperties(hasEntry(equalTo(AmqpTemporaryDestination.DYNAMIC_NODE_LIFETIME_POLICY), new DeleteOnCloseMatcher()));
        targetMatcher.withCapabilities(arrayContaining(nodeTypeCapability));

        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(Role.SENDER))
                .withSndSettleMode(equalTo(SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(notNullValue())
                .withTarget(targetMatcher);

        final AttachFrame attachResponse = new AttachFrame()
                            .setRole(Role.RECEIVER)
                            .setSndSettleMode(SenderSettleMode.UNSETTLED)
                            .setRcvSettleMode(ReceiverSettleMode.FIRST);

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, -1, attachResponse, null);
        attachResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                Object receivedHandle = attachMatcher.getReceivedHandle();

                attachResponseSender.setChannel(attachMatcher.getActualChannel());
                attachResponse.setHandle(receivedHandle);
                attachResponse.setName(attachMatcher.getReceivedName());
                attachResponse.setSource(attachMatcher.getReceivedSource());

                if (!refuseLink) {
                    Target t = (Target) createTargetObjectFromDescribedType(attachMatcher.getReceivedTarget());
                    t.setAddress(dynamicAddress);
                    attachResponse.setTarget(t);
                } else {
                    attachResponse.setTarget(null);
                }

                _lastInitiatedLinkHandle = (UnsignedInteger) receivedHandle;
            }
        });

        if (deferAttachResponseWrite)
        {
            // Defer writing the attach frame until the subsequent frame is also ready
            attachResponseSender.setDeferWrite(true);
        }

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(attachResponseSender);

        if (!refuseLink) {
            final FlowFrame flowFrame = new FlowFrame().setNextIncomingId(UnsignedInteger.ONE)  //TODO: shouldnt be hard coded
                    .setIncomingWindow(UnsignedInteger.valueOf(2048))
                    .setNextOutgoingId(UnsignedInteger.ONE) //TODO: shouldn't be hard coded
                    .setOutgoingWindow(UnsignedInteger.valueOf(2048))
                    .setLinkCredit(UnsignedInteger.valueOf(100));

            // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender flowFrameSender = new FrameSender(this, FrameType.AMQP, -1, flowFrame, null);
            flowFrameSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    flowFrameSender.setChannel(attachMatcher.getActualChannel());
                    flowFrame.setHandle(attachMatcher.getReceivedHandle());
                    flowFrame.setDeliveryCount(attachMatcher.getReceivedInitialDeliveryCount());
                }
            });

            composite.add(flowFrameSender);
        } else {
            final DetachFrame detachResponse = new DetachFrame().setClosed(true);
            if (errorType != null)
            {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();

                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);

                detachResponse.setError(detachError);
            }

            // The response frame channel will be dynamically set based on the
            // incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender detachResonseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
            detachResonseSender.setValueProvider(new ValueProvider() {
                 @Override
                 public void setValues() {
                      detachResonseSender.setChannel(attachMatcher.getActualChannel());
                      detachResponse.setHandle(attachMatcher.getReceivedHandle());
                 }
            });

            composite.add(detachResonseSender);
        }

        attachMatcher.onSuccess(composite);

        addHandler(attachMatcher);
    }

    public void expectSenderAttach()
    {
        expectSenderAttach(notNullValue(), false, false);
    }

    public void expectSenderAttach(final Matcher<?> targetMatcher, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectSenderAttach(notNullValue(), targetMatcher, refuseLink, deferAttachResponseWrite);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectSenderAttach(notNullValue(), targetMatcher, refuseLink, deferAttachResponseWrite, null, null);
    }

    public void expectSenderAttach(final Matcher<?> sourceMatcher, final Matcher<?> targetMatcher, final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(notNullValue())
                .withHandle(notNullValue())
                .withRole(equalTo(Role.SENDER))
                .withSndSettleMode(equalTo(SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(sourceMatcher)
                .withTarget(targetMatcher);

        final AttachFrame attachResponse = new AttachFrame()
                            .setRole(Role.RECEIVER)
                            .setSndSettleMode(SenderSettleMode.UNSETTLED)
                            .setRcvSettleMode(ReceiverSettleMode.FIRST);

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, -1, attachResponse, null);
        attachResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                Object receivedHandle = attachMatcher.getReceivedHandle();

                attachResponseSender.setChannel(attachMatcher.getActualChannel());
                attachResponse.setHandle(receivedHandle);
                attachResponse.setName(attachMatcher.getReceivedName());
                attachResponse.setSource(createSourceObjectFromDescribedType(attachMatcher.getReceivedSource()));
                if(refuseLink) {
                    attachResponse.setTarget(null);
                } else {
                    attachResponse.setTarget(createTargetObjectFromDescribedType(attachMatcher.getReceivedTarget()));
                }

                _lastInitiatedLinkHandle = (UnsignedInteger) receivedHandle;
            }
        });

        if(deferAttachResponseWrite)
        {
            // Defer writing the attach frame until the subsequent frame is also ready
            attachResponseSender.setDeferWrite(true);
        }

        final FlowFrame flowFrame = new FlowFrame().setNextIncomingId(UnsignedInteger.ONE) //TODO: shouldnt be hard coded
                .setIncomingWindow(UnsignedInteger.valueOf(2048))
                .setNextOutgoingId(UnsignedInteger.ONE) //TODO: shouldnt be hard coded
                .setOutgoingWindow(UnsignedInteger.valueOf(2048))
                .setLinkCredit(UnsignedInteger.valueOf(100));

        // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender flowFrameSender = new FrameSender(this, FrameType.AMQP, -1, flowFrame, null);
        flowFrameSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                flowFrameSender.setChannel(attachMatcher.getActualChannel());
                flowFrame.setHandle(attachMatcher.getReceivedHandle());
                flowFrame.setDeliveryCount(attachMatcher.getReceivedInitialDeliveryCount());
            }
        });

        final DetachFrame detachResponse = new DetachFrame().setClosed(true);
        if (errorType != null)
        {
            org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();

            detachError.setCondition(errorType);
            detachError.setDescription(errorMessage);

            detachResponse.setError(detachError);
        }

        // The response frame channel will be dynamically set based on the
        // incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender detachResonseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
        detachResonseSender.setValueProvider(new ValueProvider() {
             @Override
             public void setValues() {
                  detachResonseSender.setChannel(attachMatcher.getActualChannel());
                  detachResponse.setHandle(attachMatcher.getReceivedHandle());
             }
        });

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(attachResponseSender);
        if (refuseLink) {
            composite.add(detachResonseSender);
        } else {
            composite.add(flowFrameSender);
        }

        attachMatcher.onSuccess(composite);

        addHandler(attachMatcher);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, false, false, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean refuseLink, boolean deferAttachResponseWrite)
    {
        expectReceiverAttach(linkNameMatcher, sourceMatcher, refuseLink, deferAttachResponseWrite, null, null);
    }

    public void expectReceiverAttach(final Matcher<?> linkNameMatcher, final Matcher<?> sourceMatcher, final boolean refuseLink, boolean deferAttachResponseWrite, Symbol errorType, String errorMessage)
    {
        final AttachMatcher attachMatcher = new AttachMatcher()
                .withName(linkNameMatcher)
                .withHandle(notNullValue())
                .withRole(equalTo(Role.RECEIVER))
                .withSndSettleMode(equalTo(SenderSettleMode.UNSETTLED))
                .withRcvSettleMode(equalTo(ReceiverSettleMode.FIRST))
                .withSource(sourceMatcher)
                .withTarget(notNullValue());

        final AttachFrame attachResponse = new AttachFrame()
                            .setRole(Role.SENDER)
                            .setSndSettleMode(SenderSettleMode.UNSETTLED)
                            .setRcvSettleMode(ReceiverSettleMode.FIRST)
                            .setInitialDeliveryCount(UnsignedInteger.ZERO);

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender attachResponseSender = new FrameSender(this, FrameType.AMQP, -1, attachResponse, null);
        attachResponseSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                Object receivedHandle = attachMatcher.getReceivedHandle();

                attachResponseSender.setChannel(attachMatcher.getActualChannel());
                attachResponse.setHandle(receivedHandle);
                attachResponse.setName(attachMatcher.getReceivedName());
                attachResponse.setTarget(attachMatcher.getReceivedTarget());
                if(refuseLink) {
                    attachResponse.setSource(null);
                } else {
                    attachResponse.setSource(createSourceObjectFromDescribedType(attachMatcher.getReceivedSource()));
                }

                _lastInitiatedLinkHandle = (UnsignedInteger) receivedHandle;
            }
        });

        if(deferAttachResponseWrite)
        {
            // Defer writing the attach frame until the subsequent frame is also ready
            attachResponseSender.setDeferWrite(true);
        }

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        composite.add(attachResponseSender);

        if (refuseLink)
        {
            final DetachFrame detachResponse = new DetachFrame().setClosed(true);
            if (errorType != null)
            {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();

                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);

                detachResponse.setError(detachError);
            }

            // The response frame channel will be dynamically set based on the
            // incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender detachResonseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
            detachResonseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    detachResonseSender.setChannel(attachMatcher.getActualChannel());
                    detachResponse.setHandle(attachMatcher.getReceivedHandle());
                }
            });

            composite.add(detachResonseSender);
        }

        attachMatcher.onSuccess(composite);

        addHandler(attachMatcher);
    }

    public void expectReceiverAttach()
    {
        expectReceiverAttach(notNullValue(), notNullValue());
    }

    public void expectDurableSubscriberAttach(String topicName, String subscriptionName)
    {
        SourceMatcher sourceMatcher = new SourceMatcher();
        sourceMatcher.withAddress(equalTo(topicName));
        sourceMatcher.withDynamic(equalTo(false));
        //TODO: will possibly be changed to a 1/config durability
        sourceMatcher.withDurable(equalTo(TerminusDurability.UNSETTLED_STATE));
        sourceMatcher.withExpiryPolicy(equalTo(TerminusExpiryPolicy.NEVER));

        expectReceiverAttach(equalTo(subscriptionName), sourceMatcher);
    }

    public void expectDetach(boolean expectClosed, boolean sendResponse, boolean replyClosed)
    {
        Matcher<Boolean> closeMatcher = null;
        if(expectClosed)
        {
            closeMatcher = equalTo(true);
        }
        else
        {
            closeMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        final DetachMatcher detachMatcher = new DetachMatcher().withClosed(closeMatcher);

        if (sendResponse)
        {
            final DetachFrame detachResponse = new DetachFrame();
            if(replyClosed)
            {
                detachResponse.setClosed(replyClosed);
            }

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender detachResponseSender = new FrameSender(this, FrameType.AMQP, -1, detachResponse, null);
            detachResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    detachResponseSender.setChannel(detachMatcher.getActualChannel());
                    detachResponse.setHandle(detachMatcher.getReceivedHandle());
                }
            });

            detachMatcher.onSuccess(detachResponseSender);
        }

        addHandler(detachMatcher);
    }

    public void expectSessionFlow()
    {
        final FlowMatcher flowMatcher = new FlowMatcher()
                        .withLinkCredit(Matchers.nullValue())
                        .withHandle(Matchers.nullValue());

        addHandler(flowMatcher);
    }

    public void expectLinkFlow()
    {
        expectLinkFlow(false, false, Matchers.greaterThan(UnsignedInteger.ZERO));
    }

    public void expectLinkFlow(boolean drain, boolean sendDrainFlowResponse, Matcher<UnsignedInteger> creditMatcher)
    {
        expectLinkFlowRespondWithTransfer(null, null, null, null, null, 0, drain, sendDrainFlowResponse, creditMatcher, null);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
                                                 final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                 final PropertiesDescribedType propertiesDescribedType,
                                                 final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                 final DescribedType content)
    {
        expectLinkFlowRespondWithTransfer(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, 1);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
                                                  final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                                  final PropertiesDescribedType propertiesDescribedType,
                                                  final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                                  final DescribedType content,
                                                  final int count)
    {
        expectLinkFlowRespondWithTransfer(headerDescribedType, messageAnnotationsDescribedType, propertiesDescribedType,
                                          appPropertiesDescribedType, content, count, false, false,
                                          Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(count)), 1);
    }

    public void expectLinkFlowRespondWithTransfer(final HeaderDescribedType headerDescribedType,
            final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
            final PropertiesDescribedType propertiesDescribedType,
            final ApplicationPropertiesDescribedType appPropertiesDescribedType,
            final DescribedType content,
            final int count,
            final boolean drain,
            final boolean sendDrainFlowResponse,
            Matcher<UnsignedInteger> creditMatcher,
            final Integer nextIncomingId)
    {
        if (nextIncomingId == null && count > 0)
        {
            throw new IllegalArgumentException("The remote NextIncomingId must be specified if transfers have been requested");
        }

        Matcher<Boolean> drainMatcher = null;
        if(drain)
        {
            drainMatcher = equalTo(true);
        }
        else
        {
            drainMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        Matcher<UnsignedInteger> remoteNextIncomingIdMatcher = null;
        if(nextIncomingId != null)
        {
             remoteNextIncomingIdMatcher = Matchers.equalTo(UnsignedInteger.valueOf(nextIncomingId));
        }
        else
        {
            remoteNextIncomingIdMatcher = Matchers.greaterThanOrEqualTo(UnsignedInteger.ONE);
        }

        final FlowMatcher flowMatcher = new FlowMatcher()
                        .withLinkCredit(Matchers.greaterThanOrEqualTo(UnsignedInteger.valueOf(count)))
                        .withDrain(drainMatcher)
                        .withNextIncomingId(remoteNextIncomingIdMatcher);

        CompositeAmqpPeerRunnable composite = new CompositeAmqpPeerRunnable();
        boolean addComposite = false;

        for(int i = 0; i < count; i++)
        {
            final int nextId = nextIncomingId + i;

            String tagString = "theDeliveryTag" + nextId;
            Binary dtag = new Binary(tagString.getBytes());

            final TransferFrame transferResponse = new TransferFrame()
            .setDeliveryId(UnsignedInteger.valueOf(nextId))
            .setDeliveryTag(dtag)
            .setMessageFormat(UnsignedInteger.ZERO)
            .setSettled(false);

            Binary payload = prepareTransferPayload(headerDescribedType, messageAnnotationsDescribedType,
                    propertiesDescribedType, appPropertiesDescribedType, content);

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender transferResponseSender = new FrameSender(this, FrameType.AMQP, -1, transferResponse, payload);
            transferResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    transferResponse.setHandle(flowMatcher.getReceivedHandle());
                    transferResponseSender.setChannel(flowMatcher.getActualChannel());
                }
            });

            addComposite = true;
            composite.add(transferResponseSender);
        }

        if(drain && sendDrainFlowResponse)
        {
            final FlowFrame drainResponse = new FlowFrame();
            drainResponse.setOutgoingWindow(UnsignedInteger.ZERO); //TODO: shouldnt be hard coded
            drainResponse.setIncomingWindow(UnsignedInteger.valueOf(Integer.MAX_VALUE)); //TODO: shouldnt be hard coded
            drainResponse.setLinkCredit(UnsignedInteger.ZERO);
            drainResponse.setDrain(true);

            // The flow frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender flowResponseSender = new FrameSender(this, FrameType.AMQP, -1, drainResponse, null);
            flowResponseSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    flowResponseSender.setChannel(flowMatcher.getActualChannel());
                    drainResponse.setHandle(flowMatcher.getReceivedHandle());
                    drainResponse.setDeliveryCount(calculateNewDeliveryCount(flowMatcher));
                    drainResponse.setNextOutgoingId(calculateNewOutgoingId(flowMatcher, count));
                    drainResponse.setNextIncomingId(flowMatcher.getReceivedNextOutgoingId());
                }
            });

            addComposite = true;
            composite.add(flowResponseSender);
        }

        if(addComposite) {
            flowMatcher.onSuccess(composite);
        }

        addHandler(flowMatcher);
    }

    private UnsignedInteger calculateNewDeliveryCount(FlowMatcher flowMatcher) {
        UnsignedInteger dc = (UnsignedInteger) flowMatcher.getReceivedDeliveryCount();
        UnsignedInteger lc = (UnsignedInteger) flowMatcher.getReceivedLinkCredit();

        return dc.add(lc);
    }

    private UnsignedInteger calculateNewOutgoingId(FlowMatcher flowMatcher, int sentCount) {
        UnsignedInteger nid = (UnsignedInteger) flowMatcher.getReceivedNextIncomingId();

        return nid.add(UnsignedInteger.valueOf(sentCount));
    }

    private Binary prepareTransferPayload(final HeaderDescribedType headerDescribedType,
                                          final MessageAnnotationsDescribedType messageAnnotationsDescribedType,
                                          final PropertiesDescribedType propertiesDescribedType,
                                          final ApplicationPropertiesDescribedType appPropertiesDescribedType,
                                          final DescribedType content)
    {
        Data payloadData = Proton.data(1024);

        if(headerDescribedType != null)
        {
            payloadData.putDescribedType(headerDescribedType);
        }

        if(messageAnnotationsDescribedType != null)
        {
            payloadData.putDescribedType(messageAnnotationsDescribedType);
        }

        if(propertiesDescribedType != null)
        {
            payloadData.putDescribedType(propertiesDescribedType);
        }

        if(appPropertiesDescribedType != null)
        {
            payloadData.putDescribedType(appPropertiesDescribedType);
        }

        if(content != null)
        {
            payloadData.putDescribedType(content);
        }

        return payloadData.encode();
    }

    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher)
    {
        expectTransfer(expectedPayloadMatcher, nullValue(), false, new Accepted(), true);
    }

    //TODO: fix responseState to only admit applicable types.
    public void expectTransfer(Matcher<Binary> expectedPayloadMatcher, Matcher<?> stateMatcher, boolean settled,
                               ListDescribedType responseState, boolean responseSettled)
    {
        Matcher<Boolean> settledMatcher = null;
        if(settled)
        {
            settledMatcher = equalTo(true);
        }
        else
        {
            settledMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        final TransferMatcher transferMatcher = new TransferMatcher();
        transferMatcher.setPayloadMatcher(expectedPayloadMatcher);
        transferMatcher.withSettled(settledMatcher);
        transferMatcher.withState(stateMatcher);

        final DispositionFrame dispositionResponse = new DispositionFrame()
                                                   .setRole(Role.RECEIVER)
                                                   .setSettled(responseSettled)
                                                   .setState(responseState);

        // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
        final FrameSender dispositionFrameSender = new FrameSender(this, FrameType.AMQP, -1, dispositionResponse, null);
        dispositionFrameSender.setValueProvider(new ValueProvider()
        {
            @Override
            public void setValues()
            {
                dispositionFrameSender.setChannel(transferMatcher.getActualChannel());
                dispositionResponse.setFirst(transferMatcher.getReceivedDeliveryId());
            }
        });
        transferMatcher.onSuccess(dispositionFrameSender);

        addHandler(transferMatcher);
    }

    public void expectDispositionThatIsAcceptedAndSettled()
    {
        expectDisposition(true, new DescriptorMatcher(Accepted.DESCRIPTOR_CODE, Accepted.DESCRIPTOR_SYMBOL));
    }

    public void expectDisposition(boolean settled, Matcher<?> stateMatcher)
    {
        Matcher<Boolean> settledMatcher = null;
        if(settled)
        {
            settledMatcher = equalTo(true);
        }
        else
        {
            settledMatcher = Matchers.anyOf(equalTo(false), nullValue());
        }

        addHandler(new DispositionMatcher()
            .withSettled(settledMatcher)
            .withState(stateMatcher));
    }

    private Object createTargetObjectFromDescribedType(Object o) {
        assertThat(o, instanceOf(DescribedType.class));
        Object described = ((DescribedType) o).getDescribed();
        assertThat(described, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> targetFields = (List<Object>) described;
        Object descriptor = ((DescribedType) o).getDescriptor();
        if (descriptor == Target.DESCRIPTOR_CODE || descriptor.equals(Target.DESCRIPTOR_SYMBOL)) {
            return new Target(targetFields.toArray());
        } else if (descriptor == Coordinator.DESCRIPTOR_CODE || descriptor.equals(Coordinator.DESCRIPTOR_SYMBOL)) {
            return new Coordinator(targetFields.toArray());
        } else {
            throw new IllegalArgumentException("Unexpected target descriptor: " + descriptor);
        }
    }

    private Source createSourceObjectFromDescribedType(Object o) {
        assertThat(o, instanceOf(DescribedType.class));
        Object described = ((DescribedType) o).getDescribed();
        assertThat(described, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<Object> sourceFields = (List<Object>) described;

        return new Source(sourceFields.toArray());
    }

    public void remotelyEndLastOpenedSession(boolean expectEndResponse) {
        remotelyEndLastOpenedSession(expectEndResponse, 0, null, null);
    }

    public void remotelyEndLastOpenedSession(boolean expectEndResponse, long delayBeforeSend) {
        remotelyEndLastOpenedSession(expectEndResponse, 0, null, null);
    }

    public void remotelyEndLastOpenedSession(boolean expectEndResponse, final long delayBeforeSend, Symbol errorType, String errorMessage) {
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the End for the appropriate session
            final EndFrame endFrame = new EndFrame();
            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                endFrame.setError(detachError);
            }

            int channel = -1;
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, channel, endFrame, null);
            frameSender.setValueProvider(new ValueProvider() {
                @Override
                public void setValues() {
                    frameSender.setChannel(_lastInitiatedChannel);

                    //Insert a delay if requested
                    if (delayBeforeSend > 0) {
                        try {
                            Thread.sleep(delayBeforeSend);
                        } catch (InterruptedException e) {
                            // Ignore
                        }
                    }
                }
            });
            comp.add(frameSender);

            if (expectEndResponse) {
                // Expect a response to our End.
                final EndMatcher endMatcher = new EndMatcher();
                // TODO: enable matching on the channel number of the response.
                addHandler(endMatcher);
            }
        }
    }

    public void remotelyCloseConnection(boolean expectCloseResponse) {
        remotelyCloseConnection(expectCloseResponse, null, null, null);
    }

    public void remotelyCloseConnection(boolean expectCloseResponse, Symbol errorType, String errorMessage) {
        remotelyCloseConnection(expectCloseResponse, errorType, errorMessage, null);
    }

    public void remotelyCloseConnection(boolean expectCloseResponse, Symbol errorType, String errorMessage, Map<String, Object> info) {
        synchronized (_handlersLock) {
            // Prepare a composite to insert this action at the end of the handler sequence
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the Close
            final CloseFrame closeFrame = new CloseFrame();
            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                detachError.setInfo(info);
                closeFrame.setError(detachError);
            }

            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, CONNECTION_CHANNEL, closeFrame, null);

            comp.add(frameSender);

            if (expectCloseResponse) {
                // Expect a response to our Close.
                final CloseMatcher closeMatcher = new CloseMatcher();
                addHandler(closeMatcher);
            }
        }
    }

    public void remotelyDetachLastOpenedLinkOnLastOpenedSession(boolean expectDetachResponse, boolean closed) {
        remotelyDetachLastOpenedLinkOnLastOpenedSession(expectDetachResponse, closed, null, null);
    }

    public void remotelyDetachLastOpenedLinkOnLastOpenedSession(boolean expectDetachResponse, boolean closed, Symbol errorType, String errorMessage) {
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            // Now generate the Detach for the appropriate link on the appropriate session
            final DetachFrame detachFrame = new DetachFrame();
            detachFrame.setClosed(closed);
            if (errorType != null) {
                org.apache.qpid.jms.test.testpeer.describedtypes.Error detachError = new org.apache.qpid.jms.test.testpeer.describedtypes.Error();
                detachError.setCondition(errorType);
                detachError.setDescription(errorMessage);
                detachFrame.setError(detachError);
            }

            // The response frame channel will be dynamically set based on the previous frames. Using the -1 is an illegal placeholder.
            final FrameSender frameSender = new FrameSender(this, FrameType.AMQP, -1, detachFrame, null);
            frameSender.setValueProvider(new ValueProvider() {
                @Override
                public void setValues() {
                    frameSender.setChannel(_lastInitiatedChannel);
                    detachFrame.setHandle(_lastInitiatedLinkHandle);
                }
            });
            comp.add(frameSender);

            if (expectDetachResponse) {
                Matcher<Boolean> closeMatcher = null;
                if (closed) {
                    closeMatcher = equalTo(true);
                } else {
                    closeMatcher = Matchers.anyOf(equalTo(false), nullValue());
                }

                // Expect a response to our Detach.
                final DetachMatcher detachMatcher = new DetachMatcher().withClosed(closeMatcher);
                // TODO: enable matching on the channel number of the response.
                addHandler(detachMatcher);
            }
        }
    }

    private CompositeAmqpPeerRunnable insertCompsiteActionForLastHandler() {
        CompositeAmqpPeerRunnable comp = new CompositeAmqpPeerRunnable();
        Handler h = getLastHandler();
        AmqpPeerRunnable orig = h.getOnSuccessAction();
        if (orig != null) {
            comp.add(orig);
        }
        h.onSuccess(comp);
        return comp;
    }

    public void sendTransferToLastOpenedLinkOnLastOpenedSession() {
        synchronized (_handlersLock) {
            CompositeAmqpPeerRunnable comp = insertCompsiteActionForLastHandler();

            final int nextId = 0; //TODO: shouldn't be hard coded

            String tagString = "theDeliveryTag" + nextId;
            Binary dtag = new Binary(tagString.getBytes());

            final TransferFrame transferResponse = new TransferFrame()
            .setDeliveryId(UnsignedInteger.valueOf(nextId))
            .setDeliveryTag(dtag)
            .setMessageFormat(UnsignedInteger.ZERO)
            .setSettled(false);

            Binary payload = prepareTransferPayload(null, null, null, null, new AmqpValueDescribedType("myTextMessage"));

            // The response frame channel will be dynamically set based on the incoming frame. Using the -1 is an illegal placeholder.
            final FrameSender transferSender = new FrameSender(this, FrameType.AMQP, -1, transferResponse, payload);
            transferSender.setValueProvider(new ValueProvider()
            {
                @Override
                public void setValues()
                {
                    transferResponse.setHandle(_lastInitiatedLinkHandle);
                    transferSender.setChannel(_lastInitiatedChannel);
                }
            });

            comp.add(transferSender);
        }
    }
}
