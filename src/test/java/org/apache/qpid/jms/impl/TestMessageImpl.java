package org.apache.qpid.jms.impl;

import javax.jms.Destination;

import org.apache.qpid.jms.engine.AmqpMessage;
import org.apache.qpid.jms.engine.TestAmqpMessage;

public class TestMessageImpl extends MessageImpl<TestAmqpMessage>
{
    //message to be sent
    private TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        super(amqpMessage, sessionImpl, connectionImpl);
    }

    //message just received
    private TestMessageImpl(TestAmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        super(amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }

    @Override
    protected TestAmqpMessage prepareUnderlyingAmqpMessageForSending(TestAmqpMessage amqpMessage)
    {
        //NO-OP
        return amqpMessage;
    }


    public static MessageImpl<TestAmqpMessage> createNewMessage(SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        TestAmqpMessage testAmqpMessage = (TestAmqpMessage) TestAmqpMessage.createNewMessage();

        return new TestMessageImpl(testAmqpMessage, sessionImpl, connectionImpl);
    }

    public static MessageImpl<TestAmqpMessage> createNewMessage(AmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        if(!(amqpMessage instanceof TestAmqpMessage))
        {
            throw new IllegalArgumentException("AmqpMessage must be an instance of " + TestAmqpMessage.class.getName());
        }

        return new TestMessageImpl((TestAmqpMessage) amqpMessage, sessionImpl, connectionImpl);
    }

    public static MessageImpl<TestAmqpMessage> createReceivedMessage(AmqpMessage amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        if(!(amqpMessage instanceof TestAmqpMessage))
        {
            throw new IllegalArgumentException("AmqpMessage must be an instance of " + TestAmqpMessage.class.getName());
        }

        return new TestMessageImpl((TestAmqpMessage) amqpMessage, sessionImpl, connectionImpl, consumerDestination);
    }
}