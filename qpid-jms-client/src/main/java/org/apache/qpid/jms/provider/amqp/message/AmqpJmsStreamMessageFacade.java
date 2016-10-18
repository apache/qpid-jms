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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.jms.MessageEOFException;

import org.apache.qpid.jms.message.JmsStreamMessage;
import org.apache.qpid.jms.message.facade.JmsStreamMessageFacade;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;

/**
 * Wrapper around an AMQP Message instance that will be treated as a JMS StreamMessage
 * type.
 */
public class AmqpJmsStreamMessageFacade extends AmqpJmsMessageFacade implements JmsStreamMessageFacade {

    private List<Object> list;
    private int position = 0;

    @Override
    public AmqpJmsStreamMessageFacade copy() {
        AmqpJmsStreamMessageFacade copy = new AmqpJmsStreamMessageFacade();
        copyInto(copy);
        copy.initializeEmptyBodyList(getBody() instanceof AmqpSequence);
        copy.list.addAll(list);
        return copy;
    }

    /**
     * @return the appropriate byte value that indicates the type of message this is.
     */
    @Override
    public byte getJmsMsgType() {
        return JMS_STREAM_MESSAGE;
    }

    @Override
    public boolean hasNext() {
        return !list.isEmpty() && position < list.size();
    }

    @Override
    public Object peek() throws MessageEOFException {
        if (list.isEmpty() || position >= list.size()) {
            throw new MessageEOFException("Attempt to read past end of stream");
        }

        Object object = list.get(position);
        if (object instanceof Binary) {
            // Copy to a byte[], ensure we copy only the required portion.
            Binary bin = ((Binary) object);
            object = Arrays.copyOfRange(bin.getArray(), bin.getArrayOffset(), bin.getLength());
        }

        return object;
    }

    @Override
    public void pop() throws MessageEOFException {
        if (list.isEmpty() || position >= list.size()) {
            throw new MessageEOFException("Attempt to read past end of stream");
        }

        position++;
    }

    @Override
    public void put(Object value) {
        Object entry = value;
        if (entry instanceof byte[]) {
            entry = new Binary((byte[]) value);
        }

        list.add(entry);
    }

    @Override
    public void reset() {
        position = 0;
    }

    @Override
    public void clearBody() {
        list.clear();
        position = 0;
    }

    @Override
    public boolean hasBody() {
        return !list.isEmpty();
    }

    @Override
    public JmsStreamMessage asJmsMessage() {
        return new JmsStreamMessage(this);
    }

    @Override
    protected void initializeEmptyBody() {
        list = initializeEmptyBodyList(true);
    }

    @SuppressWarnings("unchecked")
    @Override
    void setBody(Section body) {
        if (body == null) {
            list = initializeEmptyBodyList(true);
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();

            if (value == null) {
                list = initializeEmptyBodyList(false);
            } else if (value instanceof List) {
                list = (List<Object>) value;
                super.setBody(body);
            } else {
                throw new IllegalStateException("Unexpected amqp-value body content type: " + value.getClass().getSimpleName());
            }
        } else if (body instanceof AmqpSequence) {
            List<?> value = ((AmqpSequence) body).getValue();

            if (value == null) {
                list = initializeEmptyBodyList(true);
            } else {
                list = (List<Object>) value;
                super.setBody(body);
            }
        } else {
            throw new IllegalStateException("Unexpected message body type: " + body.getClass().getSimpleName());
        }
    }

    private List<Object> initializeEmptyBodyList(boolean useSequenceBody) {
        List<Object> emptyList = new ArrayList<Object>();

        if (useSequenceBody) {
            setBody(new AmqpSequence(emptyList));
        } else {
            setBody(new AmqpValue(emptyList));
        }

        return emptyList;
    }
}
