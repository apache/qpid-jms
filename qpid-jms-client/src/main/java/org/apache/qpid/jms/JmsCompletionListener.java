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
package org.apache.qpid.jms;

import javax.jms.Message;

/**
 * Interface used to implement listeners for asynchronous {@link javax.jms.Message}
 * sends which will be notified on successful completion of a send or be notified of an
 * error that was encountered while attempting to send a {@link javax.jms.Message}.
 */
public interface JmsCompletionListener {

    /**
     * Called when an asynchronous send operation completes successfully.
     *
     * @param message
     *      the {@link javax.jms.Message} that was successfully sent.
     */
    void onCompletion(Message message);

    /**
     * Called when an asynchronous send operation fails to complete, the state
     * of the send is unknown at this point.
     *
     * @param message
     *      the {@link javax.jms.Message} that was to be sent.
     * @param exception
     *      the {@link java.lang.Exception} that describes the send error.
     */
    void onException(Message message, Exception exception);

}
