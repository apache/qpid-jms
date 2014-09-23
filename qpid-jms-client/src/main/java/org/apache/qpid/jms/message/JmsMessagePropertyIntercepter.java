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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.meta.JmsMessageId;
import org.apache.qpid.jms.util.TypeConversionSupport;

/**
 * Utility class used to intercept calls to Message property gets and sets and map the
 * correct fields in the underlying JmsMessageFacade to the property name being operated on.
 */
public class JmsMessagePropertyIntercepter {

    private static final Map<String, PropertyIntercepter> PROPERTY_INTERCEPTERS =
        new HashMap<String, PropertyIntercepter>();

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

    }

    static {
        PROPERTY_INTERCEPTERS.put("JMSXDeliveryCount", new PropertyIntercepter() {
            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new JMSException("Property JMSXDeliveryCount cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setRedeliveryCounter(rc.intValue() - 1);
            }

            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                return Integer.valueOf(message.getRedeliveryCounter() + 1);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return true;
            }
        });
        PROPERTY_INTERCEPTERS.put("JMSDestination", new PropertyIntercepter() {
            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                JmsDestination rc = (JmsDestination) TypeConversionSupport.convert(value, JmsDestination.class);
                if (rc == null) {
                    throw new JMSException("Property JMSDestination cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setDestination(rc);
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
        });
        PROPERTY_INTERCEPTERS.put("JMSReplyTo", new PropertyIntercepter() {
            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                JmsDestination rc = (JmsDestination) TypeConversionSupport.convert(value, JmsDestination.class);
                if (rc == null) {
                    throw new JMSException("Property JMSReplyTo cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setReplyTo(rc);
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
        });
        PROPERTY_INTERCEPTERS.put("JMSType", new PropertyIntercepter() {
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
        });
        PROPERTY_INTERCEPTERS.put("JMSDeliveryMode", new PropertyIntercepter() {
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
                        } else {
                            throw nfe;
                        }
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
        });
        PROPERTY_INTERCEPTERS.put("JMSPriority", new PropertyIntercepter() {
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
        });
        PROPERTY_INTERCEPTERS.put("JMSMessageID", new PropertyIntercepter() {
            @Override
            public Object getProperty(JmsMessageFacade message) throws JMSException {
                if (message.getMessageId() == null) {
                    return null;
                }
                return message.getMessageId().toString();
            }

            @Override
            public void setProperty(JmsMessageFacade message, Object value) throws JMSException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property JMSMessageID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setMessageId(new JmsMessageId(rc));
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getMessageId() != null;
            }
        });
        PROPERTY_INTERCEPTERS.put("JMSTimestamp", new PropertyIntercepter() {
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
                return true;
            }
        });
        PROPERTY_INTERCEPTERS.put("JMSCorrelationID", new PropertyIntercepter() {
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
        });
        PROPERTY_INTERCEPTERS.put("JMSExpiration", new PropertyIntercepter() {
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
                return true;
            }
        });
        PROPERTY_INTERCEPTERS.put("JMSRedelivered", new PropertyIntercepter() {
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
                return true;
            }
        });
        PROPERTY_INTERCEPTERS.put("JMSXGroupID", new PropertyIntercepter() {
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
        });
        PROPERTY_INTERCEPTERS.put("JMSXGroupSeq", new PropertyIntercepter() {
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
                return true;
            }
        });
        PROPERTY_INTERCEPTERS.put("JMSXUserID", new PropertyIntercepter() {
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
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new JMSException("Property JMSXUserID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setUserId(rc);
            }

            @Override
            public boolean propertyExists(JmsMessageFacade message) {
                return message.getUserId() != null;
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
    public static Set<String> getPropertyNames(JmsMessageFacade message) {
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
    public static void addPropertySetter(String propertyName, PropertyIntercepter getter) {
        PROPERTY_INTERCEPTERS.put(propertyName, getter);
    }

    /**
     * Given a property name, remove the configured intercepter that has been assigned to
     * intercept the queries for that property value.
     *
     * @param propertyName
     *        The name of the PropertyIntercepter to remove.
     *
     * @return true if a getter was removed from the global set.
     */
    public boolean removePropertySetter(String propertyName) {
        if (PROPERTY_INTERCEPTERS.remove(propertyName) != null) {
            return true;
        }

        return false;
    }

    private final String name;
    private final PropertyIntercepter jmsPropertyExpression;

    /**
     * Creates an new property getter instance that is assigned to read the named value.
     *
     * @param name
     *        the property value that this getter is assigned to lookup.
     */
    public JmsMessagePropertyIntercepter(String name) {
        this.name = name;
        this.jmsPropertyExpression = PROPERTY_INTERCEPTERS.get(name);
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
    public Object get(JmsMessageFacade message) throws JMSException {
        if (jmsPropertyExpression != null) {
            return jmsPropertyExpression.getProperty(message);
        }

        return message.getProperty(name);
    }

    /**
     * Sets the correct property value from the JmsMessageFacade instance based on
     * the predefined property mappings.
     *
     * @param message
     *        the JmsMessageFacade whose property is being read.
     * @param value
     *        the value to be set on the intercepted JmsMessageFacade property.
     *
     * @throws JMSException if an error occurs while reading the defined property.
     */
    public void set(JmsMessageFacade message, Object value) throws JMSException {
        if (jmsPropertyExpression != null) {
            jmsPropertyExpression.setProperty(message, value);
        } else {
            message.setProperty(name, value);
        }
    }

    /**
     * @return the property name that is being intercepted for the JmsMessageFacade.
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
        return name.equals(((JmsMessagePropertyIntercepter) o).name);
    }
}
