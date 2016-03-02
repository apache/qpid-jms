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

import java.io.IOException;
import java.io.Serializable;

import javax.jms.JMSException;

/**
 * Interface for a message Facade that wraps an ObjectMessage based
 * provider message.
 */
public interface JmsObjectMessageFacade extends JmsMessageFacade {

    /**
     * @return a deep copy of this Message Facade including a complete copy
     *         of the byte contents of the wrapped message.
     *
     * @throws JMSException if an error occurs while copying this message.
     */
    @Override
    JmsObjectMessageFacade copy() throws JMSException;

    /**
     * Gets the Object value that is contained in the provider message.
     *
     * If the Object is stored in some serialized form then the Provider must
     * de-serialize the object prior to returning it.
     *
     * @return the de-serialized version of the contained object.
     *
     * @throws IOException if the provider fails to get the object due to some internal error.
     * @throws ClassNotFoundException if object de-serialization fails because the ClassLoader
     *                                cannot find the Class locally.
     */
    Serializable getObject() throws IOException, ClassNotFoundException;

    /**
     * Stores the given object into the provider Message.
     *
     * In order for the provider to be fully JMS compliant the set Object should be
     * immediately serialized and stored so that future modifications to the object
     * are not reflected in the stored version of the object.
     *
     * @param value
     *        the new value to write to the provider message.
     *
     * @throws IOException if the provider fails to store the object due to some internal error.
     */
    void setObject(Serializable value) throws IOException;

}
