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
package org.apache.qpid.jms.message.facade;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;

/**
 * Interface for a Message Facade that wraps a stream or list based provider
 * message instance.  The interface provides the basic entry points into a
 * stream style message where primitive values are read and written as opaque
 * objects.
 */
public interface JmsStreamMessageFacade extends JmsMessageFacade {

    /**
     * @return a deep copy of this Message Facade including a complete copy
     *         of the byte contents of the wrapped message.
     */
    @Override
    JmsStreamMessageFacade copy() throws JMSException;

    /**
     * @return true if the stream contains another element beyond the current.
     */
    boolean hasNext();

    /**
     * Peek and return the next element in the stream.  If the stream has been fully read
     * then this method should throw a MessageEOFException.  Multiple calls to peek should
     * return the same element.
     *
     * @return the next value in the stream without removing it.
     *
     * @throws MessageEOFException if end of message stream has been reached.
     */
    Object peek() throws MessageEOFException;

    /**
     * Pops the next element in the stream.
     *
     * @throws MessageEOFException if end of message stream has been reached.
     */
    void pop() throws MessageEOFException;

    /**
     * Writes a new object value to the stream.
     *
     * If the value provided is a byte[] its entry then it is assumed that it was
     * copied by the caller and its value will not be altered by the provider.
     *
     * @param value
     *        The object value to be written to the stream.
     */
    void put(Object value);

    /**
     * Reset the position of the stream to the beginning.
     */
    void reset();

}
