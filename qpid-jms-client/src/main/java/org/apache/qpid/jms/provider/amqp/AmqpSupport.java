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
package org.apache.qpid.jms.provider.amqp;

import org.apache.qpid.proton.amqp.Symbol;

public class AmqpSupport {
    // Symbols used for connection capabilities
    public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");

    // Symbols used to announce connection error information
    public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
    public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

    public static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
    public static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");
}