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

import java.io.IOException;
import java.io.Serializable;

/**
 * Interface for a Delegate object that handles storing and retrieving the Object
 * value in an Object message.
 */
public interface AmqpObjectTypeDelegate {

    /**
     * Given a serializable instance, store the value into the AMQP message using
     * the strategy implemented by this delegate.
     *
     * @param value
     *        A serializable object instance to be stored in the message.
     *
     * @throws IOException if an error occurs during the store operation.
     */
    void setObject(Serializable value) throws IOException;

    /**
     * Read a Serialized object from the AMQP message using the strategy implemented
     * by this delegate.
     *
     * @return an Object that has been read from the stored object data in the message.
     *
     * @throws IOException if an error occurs while reading the stored object.
     * @throws ClassNotFoundException if no class can be found for the stored type.
     */
    Serializable getObject() throws IOException, ClassNotFoundException;

    /**
     * Signals that the message is about to be sent so we should ensure proper state of
     * the marshaled object and message annotations prior to that.
     */
    void onSend();

    /**
     * Copy the internal data into the given instance.
     *
     * @param copy
     *      the new delegate that will receive a copy of this instances object data.
     *
     * @throws Exception if an error occurs while copying the contents to the target.
     */
    void copyInto(AmqpObjectTypeDelegate copy) throws Exception;

    boolean isAmqpTypeEncoded();

    boolean hasBody();

}
