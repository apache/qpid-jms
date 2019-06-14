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
package org.apache.qpid.jms.message;

import org.apache.qpid.jms.provider.amqp.message.AmqpMessageIdHelper;

/**
 * Interface for creating a custom Message ID builder to populate the
 * Message ID field of the outgoing message.
 */
public interface JmsMessageIDBuilder {

    public enum BUILTIN {
        DEFAULT {
            @Override
            public JmsMessageIDBuilder createBuilder() {
                return new JmsMessageIDBuilder() {

                    @Override
                    public Object createMessageID(String producerId, long messageSequence) {
                        final StringBuilder idBuilder = ID_BUILDER.get();
                        idBuilder.setLength(0);
                        if (!AmqpMessageIdHelper.hasMessageIdPrefix(producerId)) {
                            idBuilder.append(AmqpMessageIdHelper.JMS_ID_PREFIX);
                        }
                        idBuilder.append(producerId).append('-').append(messageSequence);
                        return idBuilder.toString();
                    }

                    @Override
                    public String toString() {
                        return DEFAULT.name();
                    }
                };
            }
        },
        UUID {
            @Override
            public JmsMessageIDBuilder createBuilder() {
                return new JmsMessageIDBuilder() {

                    @Override
                    public Object createMessageID(String producerId, long messageSequence) {
                        return java.util.UUID.randomUUID();
                    }

                    @Override
                    public String toString() {
                        return UUID.name();
                    }
                };
            }
        },
        UUID_STRING {
            @Override
            public JmsMessageIDBuilder createBuilder() {
                return new JmsMessageIDBuilder() {

                    @Override
                    public Object createMessageID(String producerId, long messageSequence) {
                        return java.util.UUID.randomUUID().toString();
                    }

                    @Override
                    public String toString() {
                        return UUID_STRING.name();
                    }
                };
            }
        },
        PREFIXED_UUID_STRING {
            @Override
            public JmsMessageIDBuilder createBuilder() {
                return new JmsMessageIDBuilder() {

                    @Override
                    public Object createMessageID(String producerId, long messageSequence) {
                        return AmqpMessageIdHelper.JMS_ID_PREFIX + java.util.UUID.randomUUID().toString();
                    }

                    @Override
                    public String toString() {
                        return PREFIXED_UUID_STRING.name();
                    }
                };
            }
        };

        private static final ThreadLocal<StringBuilder> ID_BUILDER = ThreadLocal.withInitial(StringBuilder::new);

        public abstract JmsMessageIDBuilder createBuilder();
    }

    /**
     * Create and return a new Message ID value.  The returned
     * value must be a valid AMQP Message ID type.
     *
     * @param producerId
     *      The String ID value for the producer that is sending the message,
     * @param messageSequence
     *      The producer assigned sequence number for the outgoing message.
     *
     * @return and Object value that will be assigned as the Message ID.
     */
    Object createMessageID(String producerId, long messageSequence);

}
