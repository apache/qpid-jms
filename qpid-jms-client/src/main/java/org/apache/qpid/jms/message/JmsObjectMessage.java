/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.message;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;

import org.apache.qpid.jms.message.facade.JmsObjectMessageFacade;

/**
 * An <CODE>ObjectMessage</CODE> object is used to send a message that contains a serializable
 * object in the Java programming language ("Java object"). It inherits from the
 * <CODE>Message</CODE> interface and adds a body containing a single reference to an object.
 * Only <CODE>Serializable</CODE> Java objects can be used.
 * <p/>
 * <p/>
 * If a collection of Java objects must be sent, one of the <CODE>Collection</CODE> classes
 * provided since JDK 1.2 can be used.
 * <p/>
 * <p/>
 * When a client receives an <CODE>ObjectMessage</CODE>, it is in read-only mode. If a client
 * attempts to write to the message at this point, a <CODE>MessageNotWriteableException</CODE>
 * is thrown. If <CODE>clearBody</CODE> is called, the message can now be both read from and
 * written to.
 *
 * @see javax.jms.Session#createObjectMessage()
 * @see javax.jms.Session#createObjectMessage(Serializable)
 * @see javax.jms.BytesMessage
 * @see javax.jms.MapMessage
 * @see javax.jms.Message
 * @see javax.jms.StreamMessage
 * @see javax.jms.TextMessage
 */
public class JmsObjectMessage extends JmsMessage implements ObjectMessage {

    private final JmsObjectMessageFacade facade;

    public JmsObjectMessage(JmsObjectMessageFacade facade) {
        super(facade);
        this.facade = facade;
    }

    @Override
    public JmsObjectMessage copy() throws JMSException {
        JmsObjectMessage other = new JmsObjectMessage(facade.copy());
        other.copy(this);
        return other;
    }

    /**
     * Sets the serializable object containing this message's data. It is important to note that
     * an <CODE>ObjectMessage</CODE> contains a snapshot of the object at the time
     * <CODE>setObject()</CODE> is called; subsequent modifications of the object will have no
     * effect on the <CODE>ObjectMessage</CODE> body.
     *
     * @param newObject
     *        the message's data
     * @throws JMSException
     *         if the JMS provider fails to set the object due to some internal error.
     * @throws javax.jms.MessageFormatException
     *         if object serialization fails.
     * @throws javax.jms.MessageNotWriteableException
     *         if the message is in read-only mode.
     */
    @Override
    public void setObject(Serializable newObject) throws JMSException {
        checkReadOnlyBody();
        try {
            this.facade.setObject(newObject);
        } catch (Exception e) {
            throw new MessageFormatException("Failed to serialize object");
        }
    }

    /**
     * Gets the serializable object containing this message's data. The default value is null.
     *
     * @return the serializable object containing this message's data
     * @throws JMSException
     */
    @Override
    public Serializable getObject() throws JMSException {
        try {
            return this.facade.getObject();
        } catch (Exception e) {
            throw new MessageFormatException("Failed to read object");
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
