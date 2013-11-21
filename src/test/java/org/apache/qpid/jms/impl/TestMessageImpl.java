package org.apache.qpid.jms.impl;

import org.apache.qpid.jms.engine.TestAmqpMessage;

public class TestMessageImpl extends MessageImpl<TestAmqpMessage>
{
    public TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        super(amqpMessage,sessionImpl,connectionImpl);
    }

    @Override
    protected TestAmqpMessage prepareUnderlyingAmqpMessageForSending(
            TestAmqpMessage amqpMessage)
    {
        //TODO
        throw new UnsupportedOperationException("Not Implemented");
    }
}