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
package org.apache.qpid.jms.message.facade.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

/**
 * A test implementation of the JmsBytesMessageFacade that simply holds a raw Buffer
 */
public final class JmsTestBytesMessageFacade extends JmsTestMessageFacade implements JmsBytesMessageFacade {

    private ByteBuf content = Unpooled.EMPTY_BUFFER;
    private ByteBufOutputStream bytesOut;
    private ByteBufInputStream bytesIn;

    public JmsTestBytesMessageFacade() {
    }

    public JmsTestBytesMessageFacade(byte[] content) {
        this.content = Unpooled.copiedBuffer(content);
    }

    @Override
    public JmsMsgType getMsgType() {
        return JmsMsgType.BYTES;
    }

    @Override
    public JmsTestBytesMessageFacade copy() {
        reset();
        JmsTestBytesMessageFacade copy = new JmsTestBytesMessageFacade();
        copyInto(copy);
        if (this.content != null) {
            copy.content = this.content.copy();
        }

        return copy;
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

        content = Unpooled.EMPTY_BUFFER;
    }

    @Override
    public InputStream getInputStream() throws JMSException {
        if (bytesOut != null) {
            throw new IllegalStateException("Body is being written to, cannot perform a read.");
        }

        if (bytesIn == null) {
            // Duplicate the content buffer to allow for getBodyLength() validity.
            bytesIn = new ByteBufInputStream(content.duplicate());
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
            content = Unpooled.EMPTY_BUFFER;
        }

        return bytesOut;
    }

    @Override
    public void reset() {
        if (bytesOut != null) {
            content = bytesOut.buffer();
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
        return content.readableBytes();
    }

    @Override
    public boolean hasBody() {
        return content.isReadable() || (bytesOut != null && bytesOut.writtenBytes() > 0);
    }

    @Override
    public byte[] copyBody() {
        ByteBuf duplicate = content.duplicate();
        byte[] result = new byte[content.readableBytes()];

        duplicate.readBytes(result);

        return result;
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {
        super.onSend(producerTtl);

        reset();
    }
}
