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

import javax.jms.JMSException;

/**
 * Thrown when the remote peer return amqp:not-found for instances other
 * than those that would generate an InvalidDestinationException or some
 * other more situation appropriate JMSException instance.
 */
public class JmsResourceNotFoundException extends JMSException {

    private static final long serialVersionUID = -4811726481264524424L;

    public JmsResourceNotFoundException(String reason) {
        super(reason);
    }

    public JmsResourceNotFoundException(String reason, String errorCode) {
        super(reason, errorCode);
    }
}
