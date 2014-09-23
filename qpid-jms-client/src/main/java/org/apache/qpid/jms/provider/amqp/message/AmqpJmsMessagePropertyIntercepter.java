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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_REPLY_TO_GROUP_ID;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_TTL;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_TYPED_ENCODING;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.util.TypeConversionSupport;

/**
 * Utility class used to intercept calls to Message property sets and gets and map the
 * correct AMQP fields to the property name being accessed.
 */
public class AmqpJmsMessagePropertyIntercepter {

    private static final Map<String, PropertyIntercepter> PROPERTY_INTERCEPTERS = new HashMap<String, PropertyIntercepter>();

    /**
     * Interface for a Property intercepter object used to write JMS style
     * properties that are part of the JMS Message object members or perform
     * some needed conversion action before some named property is read or
     * written.  If a property is not writable then the intercepter should
     * throw an JMSException to indicate the error.
     */
    interface PropertyIntercepter {

        /**
         * Called when the names property is queried from an JMS Message object.
         *
         * @param message
         *        The message being acted upon.
         *
         * @return the correct property value from the given Message.
         *
         * @throws JMSException if an error occurs while accessing the property
         */
        Object getProperty(AmqpJmsMessageFacade message) throws JMSException;

        /**
         * Called when the names property is assigned from an JMS Message object.
         *
         * @param message
         *        The message instance being acted upon.
         * @param value
         *        The value to assign to the intercepted property.
         *
         * @throws JMSException if an error occurs writing the property.
         */
        void setProperty(AmqpJmsMessageFacade message, Object value) throws JMSException;

        /**
         * Indicates if the intercepted property has a value currently assigned.
         *
         * @param message
         *        The message instance being acted upon.
         *
         * @return true if the intercepted property has a value assigned to it.
         */
        boolean propertyExists(AmqpJmsMessageFacade message);

    }

    static {
        PROPERTY_INTERCEPTERS.put(JMS_AMQP_TTL, new PropertyIntercepter() {
            @Override
            public Object getProperty(AmqpJmsMessageFacade message) throws JMSException {
                return message.getAmqpTimeToLive();
            }

            @Override
            public void setProperty(AmqpJmsMessageFacade message, Object value) throws JMSException {
                Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new JMSException("Property " + JMS_AMQP_TTL + " cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setAmqpTimeToLive(rc.longValue());
            }

            @Override
            public boolean propertyExists(AmqpJmsMessageFacade message) {
                return message.getAmqpTimeToLive() != 0;
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_AMQP_REPLY_TO_GROUP_ID, new PropertyIntercepter() {
            @Override
            public Object getProperty(AmqpJmsMessageFacade message) throws JMSException {
                return message.getReplyToGroupId();
            }

            @Override
            public void setProperty(AmqpJmsMessageFacade message, Object value) throws JMSException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property " + JMS_AMQP_REPLY_TO_GROUP_ID + " cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setReplyToGroupId(rc);
            }

            @Override
            public boolean propertyExists(AmqpJmsMessageFacade message) {
                return message.getReplyToGroupId() != null;
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_AMQP_TYPED_ENCODING, new PropertyIntercepter() {
            @Override
            public Object getProperty(AmqpJmsMessageFacade message) throws JMSException {
                if (message instanceof AmqpJmsObjectMessageFacade) {
                    return ((AmqpJmsObjectMessageFacade) message).isAmqpTypedEncoding();
                }

                return false;
            }

            @Override
            public void setProperty(AmqpJmsMessageFacade message, Object value) throws JMSException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Boolean.class);
                if (rc == null) {
                    throw new JMSException("Property " + JMS_AMQP_TYPED_ENCODING + " cannot be set from a " + value.getClass().getName() + ".");
                }

                // TODO - Finished Typed encoding work.
                if (message instanceof AmqpJmsObjectMessageFacade) {
                    // ((AmqpJmsSerializedObjectMessageFacade) message)
                } else {
                    throw new MessageFormatException(JMS_AMQP_TYPED_ENCODING + " is only applicable to ObjectMessage");
                }
            }

            @Override
            public boolean propertyExists(AmqpJmsMessageFacade message) {
                if (message instanceof AmqpJmsObjectMessageFacade) {
                    // TODO - See notes in AmqpObjectMessageFacade about whether this should
                    //        always be exposed for ObjectMessage or only if it's currently
                    //        the case that the message uses the AMQP typed encoding.
                    return ((AmqpJmsObjectMessageFacade) message).isAmqpTypedEncoding();
                }

                return false;
            }
        });
    }

    /**
     * Static get method that takes a property name and gets the value either via
     * a registered property get object or through the AmqpJmsMessageFacade getProperty
     * method.
     *
     * @param message
     *        the AmqpJmsMessageFacade instance to read from
     * @param name
     *        the property name that is being requested.
     *
     * @return the correct value either mapped to an attribute of a Message or a message property.
     *
     * @throws JMSException if an error occurs while reading the defined property.
     */
    public static Object getProperty(AmqpJmsMessageFacade message, String name) throws JMSException {
        Object value = null;

        PropertyIntercepter propertyExpression = PROPERTY_INTERCEPTERS.get(name);
        if (propertyExpression != null) {
            value = propertyExpression.getProperty(message);
        } else {
            value = message.getApplicationProperty(name);
        }

        return value;
    }

    /**
     * Static set method that takes a property name and sets the value either via
     * a registered property set object or through the AmqpJmsMessageFacade setProperty
     * method.
     *
     * @param message
     *        the AmqpJmsMessageFacade instance to write to.
     * @param name
     *        the property name that is being written.
     * @param value
     *        the new value to assign for the named property.
     *
     * @throws JMSException if an error occurs while writing the defined property.
     */
    public static void setProperty(AmqpJmsMessageFacade message, String name, Object value) throws JMSException {
        PropertyIntercepter propertyExpression = PROPERTY_INTERCEPTERS.get(name);
        if (propertyExpression != null) {
            propertyExpression.setProperty(message, value);
        } else {
            message.setApplicationProperty(name, value);
        }
    }

    /**
     * Static query method to determine if a specific property exists in the given message.
     *
     * @param message
     *        the AmqpJmsMessageFacade instance to write to.
     * @param name
     *        the property name that is being checked.
     *
     * @throws JMSException if an error occurs while inspecting the defined property.
     */
    public static void propertyExists(AmqpJmsMessageFacade message, String name) throws JMSException {
        PropertyIntercepter propertyExpression = PROPERTY_INTERCEPTERS.get(name);
        if (propertyExpression != null) {
            propertyExpression.propertyExists(message);
        } else {
            message.applicationPropertyExists(name);
        }
    }

    /**
     * For each of the currently configured message property intercepter instance a
     * string key value is inserted into an Set and returned.
     *
     * @return a Set<String> containing the names of all intercepted properties.
     */
    public static Set<String> getAllPropertyNames() {
        return PROPERTY_INTERCEPTERS.keySet();
    }

    /**
     * For each of the currently configured message property intercepter instance a
     * string key value is inserted into an Set and returned if the property has a
     * value and is available for a read operation.
     *
     * @return a Set<String> containing the names of all intercepted properties with a value.
     */
    public static Set<String> getPropertyNames(AmqpJmsMessageFacade message) {
        Set<String> names = new HashSet<String>();
        for (Entry<String, PropertyIntercepter> entry : PROPERTY_INTERCEPTERS.entrySet()) {
            if (entry.getValue().propertyExists(message)) {
                names.add(entry.getKey());
            }
        }
        return names;
    }

    /**
     * Allows for the additional PropertyIntercepter instances to be added to the global set.
     *
     * @param propertyName
     *        The name of the Message property that will be intercepted.
     * @param getter
     *        The PropertyIntercepter instance that should be used for the named property.
     */
    public static void addPropertyIntercepter(String propertyName, PropertyIntercepter getter) {
        PROPERTY_INTERCEPTERS.put(propertyName, getter);
    }

    /**
     * Given a property name, remove the configured intercepter that has been assigned to
     * intercept calls for that property value.
     *
     * @param propertyName
     *        The name of the PropertyIntercepter to remove.
     *
     * @return true if a getter was removed from the global set.
     */
    public boolean removePropertyIntercepter(String propertyName) {
        if (PROPERTY_INTERCEPTERS.remove(propertyName) != null) {
            return true;
        }

        return false;
    }

    private final String name;
    private final PropertyIntercepter propertyExpression;

    /**
     * Creates an new property getter instance that is assigned to read the named value.
     *
     * @param name
     *        the property value that this getter is assigned to lookup.
     */
    public AmqpJmsMessagePropertyIntercepter(String name) {
        this.name = name;
        this.propertyExpression = PROPERTY_INTERCEPTERS.get(name);
    }

    /**
     * Gets the correct property value from the JmsMessageFacade instance based on
     * the predefined property mappings.
     *
     * @param message
     *        the JmsMessageFacade whose property is being read.
     *
     * @return the correct value either mapped to an Message attribute of a Message property.
     *
     * @throws JMSException if an error occurs while reading the defined property.
     */
    public Object get(AmqpJmsMessageFacade message) throws JMSException {
        if (propertyExpression != null) {
            return propertyExpression.getProperty(message);
        }

        return message.getApplicationProperty(name);
    }

    /**
     * Sets the correct property value from the AmqpJmsMessageFacade instance based on
     * the predefined property mappings.
     *
     * @param message
     *        the AmqpJmsMessageFacade whose property is being read.
     * @param value
     *        the value to be set on the intercepted AmqpJmsMessageFacade property.
     *
     * @throws JMSException if an error occurs while reading the defined property.
     */
    public void set(AmqpJmsMessageFacade message, Object value) throws JMSException {
        if (propertyExpression != null) {
            propertyExpression.setProperty(message, value);
        } else {
            message.setApplicationProperty(name, value);
        }
    }

    /**
     * @return the property name that is being intercepted for the AmqpJmsMessageFacade.
     */
    public String getName() {
        return name;
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || !this.getClass().equals(o.getClass())) {
            return false;
        }
        return name.equals(((AmqpJmsMessagePropertyIntercepter) o).name);
    }
}
