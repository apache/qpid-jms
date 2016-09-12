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

import java.util.ArrayList;
import java.util.List;

import javax.jms.MessageEOFException;

import org.apache.qpid.jms.message.facade.JmsStreamMessageFacade;

/**
 * Test implementation of the JmsStreamMessageFacade
 */
public class JmsTestStreamMessageFacade extends JmsTestMessageFacade implements JmsStreamMessageFacade {

    private final List<Object> stream = new ArrayList<Object>();
    private int index = -1;

    @Override
    public JmsMsgType getMsgType() {
        return JmsMsgType.STREAM;
    }

    @Override
    public JmsTestStreamMessageFacade copy() {
        JmsTestStreamMessageFacade copy = new JmsTestStreamMessageFacade();
        copyInto(copy);
        copy.stream.addAll(stream);
        return copy;
    }

    @Override
    public boolean hasNext() {
        return !stream.isEmpty() && index < stream.size();
    }

    @Override
    public Object peek() throws MessageEOFException {
        if (stream.isEmpty() || index + 1 >= stream.size()) {
            throw new MessageEOFException("Attempted to read past the end of the stream");
        }

        return stream.get(index + 1);
    }

    @Override
    public void pop() throws MessageEOFException {
        if (stream.isEmpty() || index + 1 >= stream.size()) {
            throw new MessageEOFException("Attempted to read past the end of the stream");
        }

        index++;
    }

    @Override
    public void put(Object value) {
        stream.add(value);
    }

    @Override
    public void clearBody() {
        stream.clear();
        index = -1;
    }

    @Override
    public void reset() {
        index = -1;
    }

    @Override
    public boolean hasBody() {
        return !stream.isEmpty();
    }
}
