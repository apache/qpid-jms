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
package org.apache.qpid.jms.message.facade;

import javax.jms.JMSException;

import org.fusesource.hawtbuf.Buffer;

/**
 * Interface for a Message Facade that wraps a BytesMessage based message
 * instance.
 */
public interface JmsBytesMessageFacade extends JmsMessageFacade {

    /**
     * @returns a deep copy of this Message Facade including a complete copy
     * of the byte contents of the wrapped message.
     */
    @Override
    JmsBytesMessageFacade copy() throws JMSException;

    /**
     * Retrieves the contents of this message either wrapped in or copied
     * into a Buffer instance.  If the message contents are empty a null
     * Buffer instance may be returned.
     *
     * @returns a new Buffer that contains the contents of this message.
     */
    Buffer getContent();

    /**
     * Sets the contents of the message to the new value based on the bytes
     * stored in the passed in Buffer.
     *
     * @param contents
     *        the new bytes to store in this message.
     */
    void setContent(Buffer content);

}
