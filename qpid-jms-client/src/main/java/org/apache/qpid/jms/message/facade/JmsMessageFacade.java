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
package org.apache.qpid.jms.message.facade;

import java.util.Set;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;

/**
 * The Message Facade interface defines the required mapping between a Provider's
 * own Message type and the JMS Message types.  A Provider can implement the Facade
 * interface and offer direct access to its message types without the need to
 * copy to / from a more generic JMS message instance.
 */
public interface JmsMessageFacade {

    /**
     * Returns the property names for this Message instance. The Set returned may be
     * manipulated by the receiver without impacting the facade, and an empty set
     * will be returned if there are no matching properties.
     *
     * @return a set containing the property names of this Message
     *
     * @throws JMSException if an error occurs while accessing the Message properties.
     */
    public Set<String> getPropertyNames() throws JMSException;

    /**
     * @param key
     *      The property name that is being searched for.
     *
     * @return true if the given property exists within the message.
     *
     * @throws JMSException if an error occurs while accessing the Message properties.
     */
    boolean propertyExists(String key) throws JMSException;

    /**
     * Returns the property stored in the message accessed via the given key/
     *
     * @param key
     *        the key used to access the given property.
     *
     * @return the object that is stored under the given key or null if none found.
     *
     * @throws JMSException if an error occurs while accessing the Message properties.
     */
    Object getProperty(String key) throws JMSException;

    /**
     * Sets the message property value using the supplied key to identify the value
     * that should be set or updated.
     *
     * @param key
     *        the key that identifies the message property.
     * @param value
     *        the value that is to be stored in the message.
     *
     * @throws JMSException if an error occurs while accessing the Message properties.
     */
    void setProperty(String key, Object value) throws JMSException;

    /**
     * Called before a message is sent to allow a Message instance to move the
     * contents from a logical data structure to a binary form for transmission, or
     * other processing such as setting proper message headers etc.
     *
     * The method allows for passing through producer configuration details not
     * explicitly mapped into the JMS Message allowing the facade to create the
     * most correct and compact message on the wire.
     *
     * @param producerTtl
     *        the time to live value configured on the producer when sent.
     *
     * @throws JMSException if an error occurs while preparing the message for send.
     */
    void onSend(long producerTtl) throws JMSException;

    /**
     * Called before a message is dispatched to its intended consumer to allow for
     * any necessary processing of message data such as setting read-only state etc.
     *
     * @throws JMSException if an error occurs while preparing the message for send.
     */
    void onDispatch() throws JMSException;

    /**
     * Clears the contents of this Message.
     */
    void clearBody();

    /**
     * Clears any Message properties that exist for this Message instance.
     *
     * @throws JMSException if an error occurs while accessing the message properties.
     */
    void clearProperties() throws JMSException;

    /**
     * Create a new instance and perform a deep copy of this object's
     * contents.
     *
     * @return a copy of this JmsMessageFacade instance.
     *
     * @throws JMSException if an error occurs while copying the message.
     */
    JmsMessageFacade copy() throws JMSException;

    /**
     * Gets the timestamp assigned to the message when it was sent.
     *
     * @return the message timestamp value.
     */
    long getTimestamp();

    /**
     * Sets the timestamp value of this message.
     *
     * @param timestamp
     *        the time that the message was sent by the provider.
     */
    void setTimestamp(long timestamp);

    /**
     * Returns the correlation ID set on this message if one exists, null otherwise.
     * The returned value will include the JMS mandated 'ID:' prefix if the value
     * represents a JMSMessageID rather than an application-specific string.
     *
     * @return the set correlation ID or null if not set.
     */
    String getCorrelationId();

    /**
     * Sets the correlation ID for this message.
     *
     * @param correlationId
     *        The correlation ID to set on this message, or null to clear.
     *
     * @throws JMSException if an error occurs while setting the correlation ID.
     */
    void setCorrelationId(String correlationId) throws JMSException;

    /**
     * Gets the set correlation ID of the message in raw bytes form.  If no ID was
     * set then this method may return null or an empty byte array.
     *
     * @return a byte array containing the correlation ID value in raw form.
     *
     * @throws JMSException if an error occurs while accessing the property.
     */
    byte[] getCorrelationIdBytes() throws JMSException;

    /**
     * Sets the correlation ID of the message in raw byte form.  Setting the value
     * as null or an empty byte array will clear any previously set value.  If the
     * underlying protocol cannot convert or map the given byte value to it's own
     * internal representation it should throw a JMSException indicating the error.
     *
     * @param correlationId
     *        the byte array to use to set the message correlation ID.
     */
    void setCorrelationIdBytes(byte[] correlationId);

    /**
     * Returns the message ID set on this message if one exists, null otherwise.
     * The returned value will include the JMS mandated 'ID:' prefix.
     *
     * @return the set message ID or null if not set.
     */
    String getMessageId();

    /**
     * Sets the message ID for this message.
     *
     * @param messageId
     *        The message ID to set on this message, or null to clear.
     * @throws JMSException if an error occurs while setting the message ID.
     */
    void setMessageId(String messageId) throws JMSException;

    /**
     * @return true if this message is tagged as being persistent.
     */
    boolean isPersistent();

    /**
     * Sets the persistent flag on this message.
     *
     * @param value
     *        true if the message is to be marked as persistent.
     */
    void setPersistent(boolean value);

    /**
     * Returns the current delivery count of the Message as set in the underlying
     * message instance.
     *
     * @return the current delivery count.
     */
    int getDeliveryCount();

    /**
     * Sets the delivery count on the message.
     *
     * @param deliveryCount
     *        the new delivery count to assign the Message.
     */
    void setDeliveryCount(int deliveryCount);

    /**
     * Returns the current redelivery count of the Message as set in the underlying
     * message instance.
     *
     * @return the current redelivery count.
     */
    int getRedeliveryCount();

    /**
     * Used to update the message redelivery after a local redelivery of the Message
     * has been performed.
     *
     * @param redeliveryCount
     *        the new redelivery count to assign the Message.
     */
    void setRedeliveryCount(int redeliveryCount);

    /**
     * Used to quickly check if a message has been redelivered.
     *
     * @return true if the message was redelivered, false otherwise.
     */
    boolean isRedelivered();

    /**
     * Used to set the redelivered state of a message.  This can serve to clear
     * the redelivery counter or set its initial value to one.
     *
     * @param redelivered
     *        true if the message is to be marked as redelivered, false otherwise.
     */
    void setRedelivered(boolean redelivered);

    /**
     * Returns the JMSType value as defined by the provider or set by the sending client.
     *
     * @return a String value that defines the message JMSType.
     */
    String getType();

    /**
     * Sets the String value used to define the Message JMSType by the client.
     *
     * @param type
     *        the JMSType value the client assigns to this message.
     */
    void setType(String type);

    /**
     * Returns the assigned priority value of this message in JMS ranged scoping.
     *
     * If the provider does not define a message priority value in its message objects
     * or the value is not set in the message this method should return the JMS default
     * value of 4.
     *
     * @return the priority value assigned to this message.
     */
    int getPriority();

    /**
     * Sets the message priority for this message using a JMS priority scoped value.
     *
     * @param priority
     *        the new priority value to set on this message.
     */
    void setPriority(int priority);

    /**
     * Returns the set expiration time for this message.
     *
     * The value should be returned as an absolute time given in GMT time.
     *
     * @return the time that this message expires or zero if it never expires.
     */
    long getExpiration();

    /**
     * Sets an expiration time on this message.
     *
     * The expiration time will be given as an absolute time in GMT time.
     *
     * @param expiration
     *        the time that this message should be considered as expired.
     */
    void setExpiration(long expiration);

    /**
     * Returns the set delivery time for this message.
     *
     * The value should be returned as an absolute time given in GMT time.
     *
     * @return the earliest time that the message should be made available for delivery.
     */
    long getDeliveryTime();

    /**
     * Sets an desired delivery time on this message.
     *
     * The delivery time will be given as an absolute time in GMT time.
     *
     * @param deliveryTime
     *        the earliest time that the message should be made available for delivery.
     * @param transmit
     *        whether to transmit an annotation containing the value (if non-zero)
     */
    void setDeliveryTime(long deliveryTime, boolean transmit);

    /**
     * Gets the Destination value that was assigned to this message at the time it was
     * sent.
     *
     * @return the destination to which this message was originally sent.
     */
    JmsDestination getDestination();

    /**
     * Sets the Destination that this message is being sent to.
     *
     * @param destination
     *        the destination that this message is being sent to.
     */
    void setDestination(JmsDestination destination);

    /**
     * Gets the Destination where replies for this Message are to be sent to.
     *
     * @return the reply to destination for this message or null if none set.
     */
    JmsDestination getReplyTo();

    /**
     * Sets the Destination where replies to this Message are to be sent.
     *
     * @param replyTo
     *        the Destination where replies should be sent, or null to clear.
     */
    void setReplyTo(JmsDestination replyTo);

    /**
     * Returns the ID of the user that sent this message if available.
     *
     * @return the user ID that was in use when this message was sent or null if not set.
     */
    String getUserId();

    /**
     * Sets the User ID for the connection that is being used to send this message.
     *
     * @param userId
     *        the user ID that sent this message or null to clear.
     */
    void setUserId(String userId);

    /**
     * Gets the set user ID of the message in raw bytes form.  If no ID was
     * set then this method may return null or an empty byte array.
     *
     * @return a byte array containing the user ID value in raw form.
     *
     * @throws JMSException if an error occurs while accessing the property.
     */
    byte[] getUserIdBytes() throws JMSException;

    /**
     * Sets the user ID of the message in raw byte form.  Setting the value
     * as null or an empty byte array will clear any previously set value.  If the
     * underlying protocol cannot convert or map the given byte value to it's own
     * internal representation it should throw a JMSException indicating the error.
     *
     * @param userId
     *        the byte array to use to set the message user ID.
     */
    void setUserIdBytes(byte[] userId);

    /**
     * Gets the Group ID that this message is assigned to.
     *
     * @return the Group ID this message was sent in.
     */
    String getGroupId();

    /**
     * Sets the Group ID to use for this message.
     *
     * @param groupId
     *        the Group ID that this message is assigned to.
     */
    void setGroupId(String groupId);

    /**
     * Gets the assigned group sequence of this message.
     *
     * @return the assigned group sequence of this message.
     */
    int getGroupSequence();

    /**
     * Sets the group sequence value for this message.
     *
     * @param groupSequence
     *        the group sequence value to assign this message.
     */
    void setGroupSequence(int groupSequence);

    /**
     * Returns the underlying providers message ID object for this message if one
     * exists, null otherwise. In the case the returned value is a String, it is not
     * defined whether the JMS mandated 'ID:' prefix will be present.
     *
     * @return the set provider message ID or null if not set.
     */
    Object getProviderMessageIdObject();

    /**
     * Sets the underlying providers message ID object for this message, or
     * clears it if the provided value is null.
     *
     * @param messageId
     *        The message ID to set on this message, or null to clear.
     */
    void setProviderMessageIdObject(Object messageId);

    /**
     * Returns true if the underlying message has a body, false if the body is empty.
     *
     * @return true if the underlying message has a body, false if the body is empty.
     */
    boolean hasBody();

    /**
     * Encodes the protocol level Message instance for transmission.
     *
     * @return an Object that represents the encoded form of the message for the target provider.
     */
    Object encodeMessage();

    /**
     * Returns whether the delivery time is being transmitted, i.e. incorporates an actual delivery delay.
     *
     * @return true if delivery time is being transmitted as an annotation
     */
    boolean isDeliveryTimeTransmitted();

}
