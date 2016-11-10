/*
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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.apache.qpid.jms.message.JmsBytesMessage;
import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

/**
 * A JmsBytesMessageFacade that wraps around Proton AMQP Message instances to provide
 * access to the underlying bytes contained in the message.
 */
public class AmqpJmsBytesMessageFacade extends AmqpJmsMessageFacade implements JmsBytesMessageFacade {

    private static final Binary EMPTY_BINARY = new Binary(new byte[0]);
    private static final Data EMPTY_BODY = new Data(EMPTY_BINARY);

    private transient ByteBufInputStream bytesIn;
    private transient ByteBufOutputStream bytesOut;

    @Override
    protected void initializeEmptyBody() {
        setContentType(OCTET_STREAM_CONTENT_TYPE);
        setBody(EMPTY_BODY);
    }

    @Override
    public AmqpJmsBytesMessageFacade copy() {
        reset();
        AmqpJmsBytesMessageFacade copy = new AmqpJmsBytesMessageFacade();
        copyInto(copy);

        Binary payload = getBinaryFromBody();
        copy.setContentType(OCTET_STREAM_CONTENT_TYPE);
        if (payload.getLength() > 0) {
            copy.setBody(new Data(payload));
        } else {
            copy.setBody(EMPTY_BODY);
        }

        return copy;
    }

    @Override
    public byte getJmsMsgType() {
        return JMS_BYTES_MESSAGE;
    }

    @Override
    public void clearBody() {
        if (bytesIn != null) {
            try {
                bytesIn.close();
            } catch (IOException e) {
            }
            bytesIn = null;
        }
        if (bytesOut != null) {
            try {
                bytesOut.close();
            } catch (IOException e) {
            }

            bytesOut = null;
        }

        setBody(EMPTY_BODY);
    }

    @Override
    public InputStream getInputStream() throws JMSException {
        if (bytesOut != null) {
            throw new IllegalStateException("Body is being written to, cannot perform a read.");
        }

        if (bytesIn == null) {
            Binary body = getBinaryFromBody();
            // Duplicate the content buffer to allow for getBodyLength() validity.
            bytesIn = new ByteBufInputStream(
                Unpooled.wrappedBuffer(body.getArray(), body.getArrayOffset(), body.getLength()));
        }

        return bytesIn;
    }

    @Override
    public OutputStream getOutputStream() throws JMSException {
        if (bytesIn != null) {
            throw new IllegalStateException("Body is being read from, cannot perform a write.");
        }

        if (bytesOut == null) {
            bytesOut = new ByteBufOutputStream(Unpooled.buffer());
            setBody(EMPTY_BODY);
        }

        return bytesOut;
    }

    @Override
    public void reset() {
        if (bytesOut != null) {
            ByteBuf writeBuf = bytesOut.buffer();
            Binary body = new Binary(writeBuf.array(), writeBuf.arrayOffset(), writeBuf.readableBytes());
            setBody(new Data(body));
            try {
                bytesOut.close();
            } catch (IOException e) {
            }
            bytesOut = null;
        } else if (bytesIn != null) {
            try {
                bytesIn.close();
            } catch (IOException e) {
            }
            bytesIn = null;
        }
    }

    @Override
    public int getBodyLength() {
        return getBinaryFromBody().getLength();
    }

    /**
     * Get the underlying Binary object from the body, or
     * {@link EMPTY_BINARY} if there is none. Never returns null.
     *
     * @return the body binary, or empty substitute if there is none
     */
    private Binary getBinaryFromBody() {
        Section body = getBody();
        Binary result = EMPTY_BINARY;

        if (body == null) {
            return result;
        } else if (body instanceof Data) {
            Binary payload = ((Data) body).getValue();
            if (payload != null && payload.getLength() != 0) {
                result = payload;
            }
        } else if(body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value == null) {
                return result;
            }

            if (value instanceof Binary) {
                Binary payload = (Binary)value;
                if (payload.getLength() != 0) {
                    result = payload;
                }
            } else {
                throw new java.lang.IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        } else {
            throw new java.lang.IllegalStateException("Unexpected body content type: " + body.getClass().getSimpleName());
        }

        return result;
    }

    @Override
    public boolean hasBody() {
        if (bytesOut != null) {
            return bytesOut.writtenBytes() > 0;
        } else {
            return getBinaryFromBody().getLength() != 0;
        }
    }

    @Override
    public JmsBytesMessage asJmsMessage() {
        return new JmsBytesMessage(this);
    }

    @Override
    public byte[] copyBody() {
        Binary content = getBinaryFromBody();
        byte[] result = new byte[content.getLength()];

        System.arraycopy(content.getArray(), content.getArrayOffset(), result, 0, content.getLength());

        return result;
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        super.onSend(producerTtl);

        reset();
    }
}
