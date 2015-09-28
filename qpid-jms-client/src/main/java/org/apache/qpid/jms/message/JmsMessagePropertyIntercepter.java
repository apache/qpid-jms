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
package org.apache.qpid.jms.message;

import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_DELIVERY_COUNT;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_GROUPID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_GROUPSEQ;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMSX_USERID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_CORRELATIONID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DELIVERY_MODE;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_DESTINATION;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_EXPIRATION;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_MESSAGEID;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_PRIORITY;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_REDELIVERED;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_REPLYTO;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_TIMESTAMP;
import static org.apache.qpid.jms.message.JmsMessageSupport.JMS_TYPE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.util.TypeConversionSupport;

/**
 * Utility class used to intercept calls to Message property gets and sets and map the
 * correct fields in the underlying JmsMessageFacade to the property name being operated on.
 */
public class JmsMessagePropertyIntercepter {

    private static final Map<String, PropertyIntercepter> PROPERTY_INTERCEPTERS =
        new HashMap<String, PropertyIntercepter>();
    private static final Set<String> STANDARD_HEADERS = new HashSet<String>();

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
        Object getProperty(JmsMessageFacade message) throws JMSException;

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
        void setProperty(JmsMessageFacade message, Object value) throws JMSException;

        /**
         * Indicates if the intercepted property has a value currently assigned.
         *
         * @param message
         *        The message instance being acted upon.
         *
         * @return true if the intercepted property has a value assigned to it.
         */
        boolean propertyExists(JmsMessageFacade message);

        /**
         * Request that the intercepted property be cleared.  For properties that
         * cannot be cleared the value should be set to the default value for that
         * property.
         *
         * @param message
         *        the target message object whose property should be cleared.
         *
         * @throws JMSException if an error occurs clearing the property.
         */
        void clearProperty(JmsMessageFacade message) throws JMSException;
    }

    static {
        STANDARD_HEADERS.add(JMS_MESSAGEID);
        STANDARD_HEADERS.add(JMS_TIMESTAMP);
        STANDARD_HEADERS.add(JMS_CORRELATIONID);
        STANDARD_HEADERS.add(JMS_REPLYTO);
        STANDARD_HEADERS.add(JMS_DESTINATION);
        STANDARD_HEADERS.add(JMS_DELIVERY_MODE);
        STANDARD_HEADERS.add(JMS_REDELIVERED);
        STANDARD_HEADERS.add(JMS_TYPE);
        STANDARD_HEADERS.add(JMS_EXPIRATION);
        STANDARD_HEADERS.add(JMS_PRIORITY);

        PROPERTY_INTERCEPTERS.put(JMS_DESTINATION, new PropertyIntercepter() {
            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                throw new JMSException("Cannot set JMS Destination as a property, use setJMSDestination() instead");
            }

            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                Destination dest = message.getDestination();
                if (dest == null) {
                    return null;
                }
                return dest.toString();
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getDestination() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setDestination(null);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_REPLYTO, new PropertyIntercepter() {
            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                throw new JMSException("Cannot set JMS ReplyTo as a property, use setJMSReplTo() instead");
            }

            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                if (message.getReplyTo() == null) {
                    return null;
                }
                return message.getReplyTo().toString();
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getReplyTo() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setReplyTo(null);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_TYPE, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return message.getType();
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property JMSType cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setType(rc);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getType() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setType(null);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_DELIVERY_MODE, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return message.isPersistent() ? "PERSISTENT" : "NON_PERSISTENT";
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Integer rc = null;
                try {
                    rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                } catch (NumberFormatException nfe) {
                    if (value instanceof String) {
                        if (((String) value).equalsIgnoreCase("PERSISTENT")) {
                            rc = DeliveryMode.PERSISTENT;
                        } else if (((String) value).equalsIgnoreCase("NON_PERSISTENT")) {
                            rc = DeliveryMode.NON_PERSISTENT;
                        }
                    }

                    if (rc == null) {
                        throw nfe;
                    }
                }
                if (rc == null) {
                    Boolean bool = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
                    if (bool == null) {
                        throw new JMSException("Property JMSDeliveryMode cannot be set from a " + value.getClass().getName() + ".");
                    } else {
                        message.setPersistent(bool.booleanValue());
                    }
                } else {
                    message.setPersistent(rc == DeliveryMode.PERSISTENT);
                }
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return true;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setPersistent(true); // Default value
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_PRIORITY, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return Integer.valueOf(message.getPriority());
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new JMSException("Property JMSPriority cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setPriority(rc.byteValue());
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return true;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setPriority(Message.DEFAULT_PRIORITY);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_MESSAGEID, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                if (message.getMessageId() == null) {
                    return null;
                }
                return message.getMessageId();
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property JMSMessageID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setMessageId(rc);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getMessageId() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setMessageId(null);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_TIMESTAMP, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return Long.valueOf(message.getTimestamp());
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new JMSException("Property JMSTimestamp cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setTimestamp(rc.longValue());
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getTimestamp() > 0;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setTimestamp(0);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_CORRELATIONID, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return message.getCorrelationId();
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property JMSCorrelationID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setCorrelationId(rc);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getCorrelationId() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) throws JMSException {
                message.setCorrelationId(null);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_EXPIRATION, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return Long.valueOf(message.getExpiration());
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new JMSException("Property JMSExpiration cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setExpiration(rc.longValue());
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getExpiration() > 0;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setExpiration(0);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMS_REDELIVERED, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return Boolean.valueOf(message.isRedelivered());
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Boolean rc = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
                if (rc == null) {
                    throw new JMSException("Property JMSRedelivered cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setRedelivered(rc.booleanValue());
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.isRedelivered();
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setRedelivered(false);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMSX_DELIVERY_COUNT, new PropertyIntercepter() {
            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new JMSException("Property JMSXDeliveryCount cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setDeliveryCount(rc.intValue());
            }

            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return Integer.valueOf(message.getDeliveryCount());
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return true;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setDeliveryCount(1);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMSX_GROUPID, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return message.getGroupId();
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property JMSXGroupID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setGroupId(rc);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getGroupId() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setGroupId(null);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMSX_GROUPSEQ, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return message.getGroupSequence();
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new JMSException("Property JMSXGroupSeq cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setGroupSequence(rc.intValue());
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getGroupSequence() != 0;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setGroupSequence(0);
            }
        });
        PROPERTY_INTERCEPTERS.put(JMSX_USERID, new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                Object userId = message.getUserId();
                if (userId == null) {
                    try {
                        userId = message.getProperty("JMSXUserID");
                    } catch (Exception e) {
                        throw JmsExceptionSupport.create(e);
                    }
                }

                return userId;
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                if (!(value instanceof String)) {
                    throw new JMSException("Property JMSXUserID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setUserId((String) value);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getUserId() != null;
            }

            @Override
            public void clearProperty(JmsMessageFacade message) {
                message.setUserId(null);
            }
        });
    }

    /**
     * Static get method that takes a property name and gets the value either via
     * a registered property get object or through the JmsMessageFacade getProperty
     * method.
     *
     * @param message
     *        the JmsMessageFacade instance to read from
     * @param name
     *        the property name that is being requested.
     *
     * @return the correct value either mapped to an Message attribute of a Message property.
     *
     * @throws JMSException if an error occurs while reading the defined property.
     */
    public static Object getProperty(JmsMessageFacade message, String name) throws JMSException {
        Object value = null;

        PropertyIntercepter jmsPropertyExpression = PROPERTY_INTERCEPTERS.get(name);
        if (jmsPropertyExpression != null) {
            value = jmsPropertyExpression.getProperty(message);
        } else {
            value = message.getProperty(name);
        }

        return value;
    }

    /**
     * Static set method that takes a property name and sets the value either via
     * a registered property set object or through the JmsMessageFacade setProperty
     * method.
     *
     * @param message
     *        the JmsMessageFacade instance to write to.
     * @param name
     *        the property name that is being written.
     * @param value
     *        the new value to assign for the named property.
     *
     * @throws JMSException if an error occurs while writing the defined property.
     */
    public static void setProperty(JmsMessageFacade message, String name, Object value) throws JMSException {
        PropertyIntercepter jmsPropertyExpression = PROPERTY_INTERCEPTERS.get(name);
        if (jmsPropertyExpression != null) {
            jmsPropertyExpression.setProperty(message, value);
        } else {
            message.setProperty(name, value);
        }
    }

    /**
     * Static inspection method to determine if a named property exists for a given message.
     *
     * @param message
     *        the JmsMessageFacade instance to read from
     * @param name
     *        the property name that is being inspected.
     *
     * @return true if the message contains the given property.
     *
     * @throws JMSException if an error occurs while validating the defined property.
     */
    public static boolean propertyExists(JmsMessageFacade message, String name) throws JMSException {
        PropertyIntercepter jmsPropertyExpression = PROPERTY_INTERCEPTERS.get(name);
        if (jmsPropertyExpression != null) {
            return jmsPropertyExpression.propertyExists(message);
        } else {
            return message.propertyExists(name);
        }
    }

    /**
     * For each of the currently configured message property intercepter instances clear or
     * reset the value to its default.  Once complete the method will direct the given provider
     * message facade to clear any message properties that might have been set.
     *
     * @param message
     *        the JmsMessageFacade instance to read from
     * @param excludeStandardJMSHeaders
     *        whether the standard JMS header names should be excluded from the returned set
     *
     * @throws JMSException if an error occurs while validating the defined property.
     */
    public static void clearProperties(JmsMessageFacade message, boolean excludeStandardJMSHeaders) throws JMSException {
        for (Entry<String, PropertyIntercepter> entry : PROPERTY_INTERCEPTERS.entrySet()) {
            if (excludeStandardJMSHeaders && STANDARD_HEADERS.contains(entry.getKey())) {
                continue;
            }

            entry.getValue().clearProperty(message);
        }

        message.clearProperties();
    }

    /**
     * For each of the currently configured message property intercepter instance a
     * string key value is inserted into an Set and returned.
     *
     * @param message
     *        the JmsMessageFacade instance to read property names from.
     *
     * @return a {@code Set<String>} containing the names of all intercepted properties.
     *
     * @throws JMSException if an error occurs while gathering the message property names.
     */
    public static Set<String> getAllPropertyNames(JmsMessageFacade message) throws JMSException {
        Set<String> names = new HashSet<String>(PROPERTY_INTERCEPTERS.keySet());
        names.addAll(message.getPropertyNames());
        return names;
    }

    /**
     * For each of the currently configured message property intercepter instance a
     * string key value is inserted into an Set and returned if the property has a
     * value and is available for a read operation. The Set returned may be
     * manipulated by the receiver without impacting the facade, and an empty set
     * will be returned if there are no matching properties.
     *
     * @param message
     *        the JmsMessageFacade instance to read from
     * @param excludeStandardJMSHeaders
     *        whether the standard JMS header names should be excluded from the returned set
     *
     * @return a {@code Set<String>} containing the names of all intercepted properties with a value.
     *
     * @throws JMSException if an error occurs while gathering the message property names.
     */
    public static Set<String> getPropertyNames(JmsMessageFacade message, boolean excludeStandardJMSHeaders) throws JMSException {
        Set<String> names = new HashSet<String>();
        for (Entry<String, PropertyIntercepter> entry : PROPERTY_INTERCEPTERS.entrySet()) {
            if (excludeStandardJMSHeaders && STANDARD_HEADERS.contains(entry.getKey())) {
                continue;
            }

            if (entry.getValue().propertyExists(message)) {
                names.add(entry.getKey());
            }
        }

        names.addAll(message.getPropertyNames());

        return names;
    }
}
