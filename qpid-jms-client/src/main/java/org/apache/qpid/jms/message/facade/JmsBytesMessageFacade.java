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

import java.io.InputStream;
import java.io.OutputStream;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

/**
 * Interface for a Message Facade that wraps a BytesMessage based message
 * instance.
 */
public interface JmsBytesMessageFacade extends JmsMessageFacade {

    /**
     * Performs a copy of this message facade into a new instance.  Calling this method
     * results in a call to reset() prior to the message copy meaning any in use streams
     * will be closed on return.
     *
     * @return a deep copy of this Message Facade including a complete copy
     *         of the byte contents of the wrapped message.
     */
    @Override
    JmsBytesMessageFacade copy() throws JMSException;

    /**
     * Create and return an InputStream instance that can be used to read the contents
     * of this message.  If an OutputStream was previously created and no call to reset
     * has yet been made then this method will throw an exception.
     *
     * Multiple calls to this method should return the same InputStream instance, only
     * when the message has been reset should the current input stream instance be discarded
     * and a new one created on demand.  While this means the multiple concurrent readers
     * is possible it is strongly discouraged.
     *
     * If the message body contains data that has been compressed and can be determined
     * to be so by the implementation then this method will return an InputStream instance
     * that can inflate the compressed data.
     *
     * @return an InputStream instance to read the message body.
     *
     * @throws JMSException if an error occurs creating the stream.
     * @throws IllegalStateException if there is a current OutputStream in use.
     */
    InputStream getInputStream() throws JMSException;

    /**
     * Create and return a new OuputStream used to populate the body of the message. If an
     * InputStream was previously requested this method will fail until such time as a call
     * to reset has been requested.
     *
     * If an existing OuputStream has already been created then this method will return
     * that stream until such time as the reset method has been called.
     *
     * @return an OutputStream instance to write the message body.
     *
     * @throws JMSException if an error occurs creating the stream.
     * @throws IllegalStateException if there is a current OutputStream in use.
     */
    OutputStream getOutputStream() throws JMSException;

    /**
     * Reset the message state such that a call to getInputStream or getOutputStream
     * will succeed.  If an OutputStream instance exists it is closed an the current
     * contents are stored into the message body.
     */
    void reset();

    /**
     * @return the number of bytes contained in the body of the message.
     */
    int getBodyLength();

    /**
     * @return a copy of the bytes contained in the body of the message.
     */
    byte[] copyBody();

}
