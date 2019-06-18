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

import java.util.Locale;

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

                /**
                 * The default builder is meant to be used as a single instance per producer
                 * and will yield incorrect results if used across multiple producer instances.
                 */
                return new JmsMessageIDBuilder() {

                    private final StringBuilder builder = new StringBuilder();
                    private int idPrefixLength = -1;

                    @Override
                    public Object createMessageID(String producerId, long messageSequence) {
                        if (idPrefixLength < 0) {
                            initialize(producerId);
                        }

                        builder.setLength(idPrefixLength);
                        builder.append(messageSequence);

                        return builder.toString();
                    }

                    @Override
                    public JmsMessageIDBuilder initialize(String producerId) {
                        if (!AmqpMessageIdHelper.hasMessageIdPrefix(producerId)) {
                            builder.append(AmqpMessageIdHelper.JMS_ID_PREFIX);
                        }
                        builder.append(producerId).append("-");

                        idPrefixLength = builder.length();

                        return this;
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

        public abstract JmsMessageIDBuilder createBuilder();

        /**
         * Creates a new JmsMessageIDBuilder from the named type (case insensitive).
         *
         * @param value
         *      The name of the builder to create.
         *
         * @return a new JmsMessageIDBuilder that matches the named type.
         *
         * @throws IllegalArgumentException if the named type is unknown.
         */
        public static JmsMessageIDBuilder create(String value) {
            return validate(value).createBuilder();
        }

        /**
         * Validates the value given maps to the built in message ID builders and
         * return the builder enumeration that it maps to which can later be used
         * to create builders of that type.
         *
         * @param value
         * 		The name of one of the built in message ID builders.
         *
         * @return the enumeration value that maps to the built in builder.
         */
        public static BUILTIN validate(String value) {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        }
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

    /**
     * Provides an initialization point for Message ID builders in order for them
     * to be able to better optimize the creation of the ID values depending on the
     * implementation of the builder.
     *
     * For builders that are created in a one per producer fashion this methods is
     * given the producer Id that the builder will be assigned to which can allow
     * for caching a custom id encoding etc.
     *
     * @param producerId
     * 		The unique Id of a producer object that will be using the builder.
     *
     * @return the builder instance for chaining.
     */
    default JmsMessageIDBuilder initialize(String producerId) { return this; }

}
