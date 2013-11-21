package org.apache.qpid.jms.engine;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class TestAmqpMessage extends AmqpMessage
{
    public TestAmqpMessage()
    {
    }

    public TestAmqpMessage(Message message, Delivery delivery, AmqpConnection amqpConnection)
    {
        super(message, delivery, amqpConnection);
    }
}