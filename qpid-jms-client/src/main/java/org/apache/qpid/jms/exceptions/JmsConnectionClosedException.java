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
package org.apache.qpid.jms.exceptions;

import javax.jms.IllegalStateException;

/**
 * An exception thrown when attempt is made to use a connection when the connection has been closed.
 */
public class JmsConnectionClosedException extends IllegalStateException {

    private static final long serialVersionUID = -7975982446284065025L;

    public JmsConnectionClosedException(Exception cause) {
        super("The JMS connection has been closed: " + extractMessage(cause));
        initCause(cause);
        setLinkedException(cause);
    }

    public JmsConnectionClosedException() {
        super("The JMS connection has been closed", "AlreadyClosed");
    }

    private static String extractMessage(Exception cause) {
        String m = cause.getMessage();
        if (m == null || m.length() == 0) {
            m = cause.toString();
        }
        return m;
    }
}
