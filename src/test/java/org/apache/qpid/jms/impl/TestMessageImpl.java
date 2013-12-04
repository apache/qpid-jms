package org.apache.qpid.jms.impl;

import javax.jms.Destination;

import org.apache.qpid.jms.engine.TestAmqpMessage;

public class TestMessageImpl extends MessageImpl<TestAmqpMessage>
{
    //message to be sent
    public TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        super(amqpMessage, sessionImpl, connectionImpl);
    }

    //message just recieved
    public TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        super(amqpMessage, sessionImpl, connectionImpl, null);
    }

    @Override
    protected TestAmqpMessage prepareUnderlyingAmqpMessageForSending(TestAmqpMessage amqpMessage)
    {
        //NO-OP
        return amqpMessage;
    }
}