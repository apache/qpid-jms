package org.apache.qpid.jms.engine;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class TestAmqpMessage extends AmqpMessage
{
    //message to be sent
    private TestAmqpMessage()
    {
        super();
    }

    //message just received
    private TestAmqpMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        super(message, delivery, amqpConnection);
    }

    public static AmqpMessage createNewMessage()
    {
        return new TestAmqpMessage();
    }

    public static AmqpMessage createReceivedMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        return new TestAmqpMessage(message, delivery, amqpConnection);
    }
}