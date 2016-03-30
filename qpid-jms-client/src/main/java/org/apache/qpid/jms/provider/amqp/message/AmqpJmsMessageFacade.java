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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_AMQP_TTL;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.exceptions.IdConversionException;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.message.Message;

public class AmqpJmsMessageFacade implements JmsMessageFacade {

    private static final int DEFAULT_PRIORITY = javax.jms.Message.DEFAULT_PRIORITY;
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final long UINT_MAX = 0xFFFFFFFFL;

    protected final Message message;
    protected final AmqpConnection connection;

    private Map<Symbol,Object> messageAnnotationsMap;
    private Map<String,Object> applicationPropertiesMap;

    private JmsDestination replyTo;
    private JmsDestination destination;
    private JmsDestination consumerDestination;

    private Long syntheticExpiration;

    /**
     * Used to record the value of JMS_AMQP_TTL property
     * if it is explicitly set by the application
     */
    private Long userSpecifiedTTL = null;

    /**
     * Create a new AMQP Message Facade with an empty message instance.
     *
     * @param connection
     *        the AmqpConnection that under which this facade was created.
     */
    public AmqpJmsMessageFacade(AmqpConnection connection) {
        this.message = Proton.message();
        this.message.setDurable(true);

        this.connection = connection;
        setMessageAnnotation(JMS_MSG_TYPE, JMS_MESSAGE);
    }

    /**
     * Creates a new Facade around an incoming AMQP Message for dispatch to the
     * JMS Consumer instance.
     *
     * @param consumer
     *        the consumer that received this message.
     * @param message
     *        the incoming Message instance that is being wrapped.
     */
    @SuppressWarnings("unchecked")
    public AmqpJmsMessageFacade(AmqpConsumer consumer, Message message) {
        this.message = message;
        this.connection = consumer.getConnection();
        this.consumerDestination = consumer.getDestination();

        if (message.getMessageAnnotations() != null) {
            messageAnnotationsMap = message.getMessageAnnotations().getValue();
        }

        if (message.getApplicationProperties() != null) {
            applicationPropertiesMap = message.getApplicationProperties().getValue();
        }

        Long ttl = getTtl();
        Long absoluteExpiryTime = getAbsoluteExpiryTime();
        if (absoluteExpiryTime == null && ttl != null) {
            syntheticExpiration = System.currentTimeMillis() + ttl;
        }
    }

    /**
     * @return the appropriate byte value that indicates the type of message this is.
     */
    public byte getJmsMsgType() {
        return JMS_MESSAGE;
    }

    /**
     * The annotation value for the JMS Message content type.  For a generic JMS message this
     * value is omitted so we return null here, subclasses should override this to return the
     * correct content type value for their payload.
     *
     * @return a String value indicating the message content type.
     */
    public String getContentType() {
        return message.getContentType();
    }

    public void setContentType(String value) {
        message.setContentType(value);
    }

    @Override
    public boolean propertyExists(String key) throws JMSException {
        return AmqpJmsMessagePropertyIntercepter.propertyExists(this, key);
    }

    public boolean applicationPropertyExists(String key) throws JMSException {
        if (applicationPropertiesMap != null) {
            return applicationPropertiesMap.containsKey(key);
        }

        return false;
    }

    /**
     * Returns a set of all the property names that have been set in this message.
     * The Set returned may be manipulated by the receiver without impacting the facade,
     * and an empty set will be returned if there are no matching properties.
     *
     * @return a set of property names in the message or an empty set if none are set.
     */
    @Override
    public Set<String> getPropertyNames() {
        return AmqpJmsMessagePropertyIntercepter.getPropertyNames(this);
    }

    public Set<String> getApplicationPropertyNames(Set<String> propertyNames) {
        if (applicationPropertiesMap != null) {
            propertyNames.addAll(applicationPropertiesMap.keySet());
        }

        return propertyNames;
    }

    @Override
    public Object getProperty(String key) throws JMSException {
        return AmqpJmsMessagePropertyIntercepter.getProperty(this, key);
    }

    public Object getApplicationProperty(String key) throws JMSException {
        if (applicationPropertiesMap != null) {
            return applicationPropertiesMap.get(key);
        }

        return null;
    }

    @Override
    public void setProperty(String key, Object value) throws JMSException {
        if (key == null) {
            throw new IllegalArgumentException("Property key must not be null");
        }

        AmqpJmsMessagePropertyIntercepter.setProperty(this, key, value);
    }

    public void setApplicationProperty(String key, Object value) throws JMSException {
        lazyCreateApplicationProperties();
        applicationPropertiesMap.put(key, value);
    }

    @Override
    public void onSend(long producerTtl) throws JMSException {

        // Set the ttl field of the Header field if needed, complementing the expiration
        // field of Properties for any peers that only inspect the mutable ttl field.
        long ttl = 0;
        if (hasAmqpTimeToLiveOverride()) {
            ttl = getAmqpTimeToLiveOverride();
        } else {
            ttl = producerTtl;
        }

        if (ttl > 0 && ttl < UINT_MAX) {
            message.setTtl(ttl);
        } else {
            Header hdr = message.getHeader();
            if (hdr != null) {
                hdr.setTtl(null);
            }
        }

        setMessageAnnotation(JMS_MSG_TYPE, getJmsMsgType());
    }

    @Override
    public void onDispatch() throws JMSException {
    }

    @Override
    public void clearBody() {
        message.setBody(null);
    }

    @Override
    public void clearProperties() throws JMSException {
        AmqpJmsMessagePropertyIntercepter.clearProperties(this);
    }

    @Override
    public AmqpJmsMessageFacade copy() throws JMSException {
        AmqpJmsMessageFacade copy = new AmqpJmsMessageFacade(connection);
        copyInto(copy);
        return copy;
    }

    @SuppressWarnings("unchecked")
    protected void copyInto(AmqpJmsMessageFacade target) {
        if (consumerDestination != null) {
            target.consumerDestination = consumerDestination;
        }

        if (destination != null) {
            target.setDestination(destination);
        }

        if (replyTo != null) {
            target.setReplyTo(replyTo);
        }

        if (syntheticExpiration != null) {
            target.syntheticExpiration = syntheticExpiration;
        }

        if (userSpecifiedTTL != null) {
            target.userSpecifiedTTL = userSpecifiedTTL;
        }

        Message targetMsg = target.getAmqpMessage();

        if (message.getHeader() != null) {
            Header headers = new Header();
            headers.setDurable(message.getHeader().getDurable());
            headers.setPriority(message.getHeader().getPriority());
            headers.setTtl(message.getHeader().getTtl());
            headers.setFirstAcquirer(message.getHeader().getFirstAcquirer());
            headers.setDeliveryCount(message.getHeader().getDeliveryCount());
            targetMsg.setHeader(headers);
        }

        if (message.getFooter() != null && message.getFooter().getValue() != null) {
            Map<Object, Object> newFooterMap = new HashMap<Object, Object>();
            newFooterMap.putAll(message.getFooter().getValue());
            targetMsg.setFooter(new Footer(newFooterMap));
        }

        if (message.getProperties() != null) {
            Properties properties = new Properties();

            properties.setMessageId(message.getProperties().getMessageId());
            properties.setUserId(message.getProperties().getUserId());
            properties.setTo(message.getProperties().getTo());
            properties.setSubject(message.getProperties().getSubject());
            properties.setReplyTo(message.getProperties().getReplyTo());
            properties.setCorrelationId(message.getProperties().getCorrelationId());
            properties.setContentType(message.getProperties().getContentType());
            properties.setContentEncoding(message.getProperties().getContentEncoding());
            properties.setAbsoluteExpiryTime(message.getProperties().getAbsoluteExpiryTime());
            properties.setCreationTime(message.getProperties().getCreationTime());
            properties.setGroupId(message.getProperties().getGroupId());
            properties.setGroupSequence(message.getProperties().getGroupSequence());
            properties.setReplyToGroupId(message.getProperties().getReplyToGroupId());

            targetMsg.setProperties(properties);
        }

        if (message.getDeliveryAnnotations() != null && message.getDeliveryAnnotations().getValue() != null) {
            Map<Symbol, Object> newDeliveryAnnotations = new HashMap<Symbol, Object>();
            newDeliveryAnnotations.putAll(message.getDeliveryAnnotations().getValue());
            targetMsg.setFooter(new Footer(newDeliveryAnnotations));
        }

        if (applicationPropertiesMap != null) {
            target.lazyCreateApplicationProperties();
            target.applicationPropertiesMap.putAll(applicationPropertiesMap);
        }

        if (messageAnnotationsMap != null) {
            target.lazyCreateMessageAnnotations();
            target.messageAnnotationsMap.putAll(messageAnnotationsMap);
        }
    }

    @Override
    public String getMessageId() {
        Object underlying = message.getMessageId();
        AmqpMessageIdHelper helper = AmqpMessageIdHelper.INSTANCE;
        String baseStringId = helper.toBaseMessageIdString(underlying);

        // Ensure the ID: prefix is present.
        // TODO: should we always do this when non-null? AMQP JMS Mapping says never to send the "ID:" prefix.
        if (baseStringId != null && !helper.hasMessageIdPrefix(baseStringId)) {
            baseStringId = AmqpMessageIdHelper.JMS_ID_PREFIX + baseStringId;
        }

        return baseStringId;
    }

    @Override
    public Object getProviderMessageIdObject() {
        return message.getMessageId();
    }

    @Override
    public void setProviderMessageIdObject(Object messageId) {
        message.setMessageId(messageId);
    }

    @Override
    public void setMessageId(String messageId) {
        if (messageId == null) {
            message.setMessageId(null);
        } else {
            // Remove the first 'ID:' prefix if present
            String stripped = AmqpMessageIdHelper.INSTANCE.stripMessageIdPrefix(messageId);
            message.setMessageId(stripped);
        }
    }

    @Override
    public long getTimestamp() {
        if (message.getProperties() != null) {
            Date timestamp = message.getProperties().getCreationTime();
            if (timestamp != null) {
                return timestamp.getTime();
            }
        }

        return 0L;
    }

    @Override
    public void setTimestamp(long timestamp) {
        if (timestamp != 0) {
            message.setCreationTime(timestamp);
        } else {
            if (message.getProperties() != null) {
                message.getProperties().setCreationTime(null);
            }
        }
    }

    @Override
    public String getCorrelationId() {
        AmqpMessageIdHelper messageIdHelper = AmqpMessageIdHelper.INSTANCE;
        String baseIdString = messageIdHelper.toBaseMessageIdString(message.getCorrelationId());

        if (baseIdString == null) {
            return null;
        } else {
            Object annotation = getMessageAnnotation(AmqpMessageSupport.JMS_APP_CORRELATION_ID);
            boolean appSpecific = Boolean.TRUE.equals(annotation);

            if (appSpecific) {
                return baseIdString;
            } else {
                return AmqpMessageIdHelper.JMS_ID_PREFIX + baseIdString;
            }
        }
    }

    @Override
    public void setCorrelationId(String correlationId) throws IdConversionException {
        AmqpMessageIdHelper messageIdHelper = AmqpMessageIdHelper.INSTANCE;
        boolean appSpecific = false;
        if (correlationId == null) {
            message.setCorrelationId(null);
        } else {
            boolean hasMessageIdPrefix = messageIdHelper.hasMessageIdPrefix(correlationId);
            if (!hasMessageIdPrefix) {
                appSpecific = true;
            }

            String stripped = messageIdHelper.stripMessageIdPrefix(correlationId);

            if (hasMessageIdPrefix) {
                Object idObject = messageIdHelper.toIdObject(stripped);
                message.setCorrelationId(idObject);
            } else {
                message.setCorrelationId(stripped);
            }
        }

        if (appSpecific) {
            setMessageAnnotation(AmqpMessageSupport.JMS_APP_CORRELATION_ID, true);
        } else {
            removeMessageAnnotation(AmqpMessageSupport.JMS_APP_CORRELATION_ID);
        }
    }

    @Override
    public byte[] getCorrelationIdBytes() throws JMSException {
        Object correlationId = message.getCorrelationId();
        if (correlationId == null) {
            return null;
        } else if (correlationId instanceof Binary) {
            ByteBuffer dup = ((Binary) correlationId).asByteBuffer();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return bytes;
        } else {
            // TODO - Do we need to throw here, or could we just stringify whatever is in
            //        there and return the UTF-8 bytes?  This method is pretty useless so
            //        maybe we just return something and let the user sort if out if they
            //        really think they need this.
            throw new JMSException("The underlying correlation-id is not binary and so can't be returned");
        }
    }

    @Override
    public void setCorrelationIdBytes(byte[] correlationId) {
        Binary binaryIdValue = null;
        if (correlationId != null) {
            binaryIdValue = new Binary(Arrays.copyOf(correlationId, correlationId.length));
        }

        message.setCorrelationId(binaryIdValue);
    }

    @Override
    public boolean isPersistent() {
        return message.isDurable();
    }

    @Override
    public void setPersistent(boolean value) {
        this.message.setDurable(value);
    }

    @Override
    public int getDeliveryCount() {
        return getRedeliveryCount() + 1;
    }

    @Override
    public void setDeliveryCount(int deliveryCount) {
        setRedeliveryCount(deliveryCount - 1);
    }

    @Override
    public int getRedeliveryCount() {
        if (message.getHeader() != null) {
            UnsignedInteger count = message.getHeader().getDeliveryCount();
            if (count != null) {
                return count.intValue();
            }
        }

        return 0;
    }

    @Override
    public void setRedeliveryCount(int redeliveryCount) {
        if (redeliveryCount == 0) {
            if (message.getHeader() != null) {
                message.getHeader().setDeliveryCount(null);
            }
        } else {
            message.setDeliveryCount(redeliveryCount);
        }
    }

    @Override
    public boolean isRedelivered() {
        return getRedeliveryCount() > 0;
    }

    @Override
    public void setRedelivered(boolean redelivered) {
        if (redelivered) {
            if (!isRedelivered()) {
                setRedeliveryCount(1);
            }
        } else {
            if (isRedelivered()) {
                setRedeliveryCount(0);
            }
        }
    }

    @Override
    public String getType() {
        return message.getSubject();
    }

    @Override
    public void setType(String type) {
        if (type != null) {
            message.setSubject(type);
        } else {
            if (message.getProperties() != null) {
                message.getProperties().setSubject(null);
            }
        }
    }

    @Override
    public int getPriority() {
        if (message.getHeader() != null) {
            UnsignedByte priority = message.getHeader().getPriority();
            if (priority != null) {
                int scaled = priority.intValue();
                if (scaled > 9) {
                    scaled = 9;
                }

                return scaled;
            }
        }

        return DEFAULT_PRIORITY;
    }

    @Override
    public void setPriority(int priority) {
        if (priority == DEFAULT_PRIORITY) {
            if (message.getHeader() == null) {
                return;
            } else {
                message.getHeader().setPriority(null);
            }
        } else {
            byte scaled = (byte) priority;
            if (priority < 0) {
                scaled = 0;
            } else if (priority > 9) {
                scaled = 9;
            }

            message.setPriority(scaled);
        }
    }

    @Override
    public long getExpiration() {
        Long absoluteExpiry = getAbsoluteExpiryTime();
        if (absoluteExpiry != null) {
            return absoluteExpiry;
        }

        if (syntheticExpiration != null) {
            return syntheticExpiration;
        }

        return 0;
    }

    @Override
    public void setExpiration(long expiration) {
        syntheticExpiration = null;

        if (expiration != 0) {
            setAbsoluteExpiryTime(expiration);
        } else {
            setAbsoluteExpiryTime(null);
        }
    }

    /**
     * Sets a value which will be used to override any ttl value that may otherwise be set
     * based on the expiration value when sending the underlying AMQP message. A value of 0
     * means to clear the ttl field rather than set it to anything.
     *
     * @param ttl
     *        the value to use, in range {@literal 0 <= x <= 2^32 - 1}
     *
     * @throws MessageFormatException if the TTL value is not in the allowed range.
     */
    public void setAmqpTimeToLiveOverride(Long ttl) throws MessageFormatException {
        if (ttl != null) {
            if (ttl >= 0 && ttl <= UINT_MAX) {
                userSpecifiedTTL = ttl;
            } else {
                throw new MessageFormatException(JMS_AMQP_TTL + " must be a long with value in range 0 to 2^32 - 1");
            }
        } else {
            userSpecifiedTTL = null;
        }
    }

    public boolean hasAmqpTimeToLiveOverride() {
        return userSpecifiedTTL != null;
    }

    public long getAmqpTimeToLiveOverride() {
        return userSpecifiedTTL != null ? userSpecifiedTTL : 0;
    }

    @Override
    public JmsDestination getDestination() {
        if (destination == null) {
            this.destination = AmqpDestinationHelper.INSTANCE.getJmsDestination(this, consumerDestination);
        }

        return destination;
    }

    @Override
    public void setDestination(JmsDestination destination) {
        this.destination = destination;
        lazyCreateMessageAnnotations();
        AmqpDestinationHelper.INSTANCE.setToAddressFromDestination(this, destination);
    }

    @Override
    public JmsDestination getReplyTo() {
        if (replyTo == null) {
            replyTo = AmqpDestinationHelper.INSTANCE.getJmsReplyTo(this, consumerDestination);
        }

        return replyTo;
    }

    @Override
    public void setReplyTo(JmsDestination replyTo) {
        this.replyTo = replyTo;
        lazyCreateMessageAnnotations();
        AmqpDestinationHelper.INSTANCE.setReplyToAddressFromDestination(this, replyTo);
    }

    public void setReplyToGroupId(String replyToGroupId) {
        message.setReplyToGroupId(replyToGroupId);
    }

    public String getReplyToGroupId() {
        return message.getReplyToGroupId();
    }

    @Override
    public String getUserId() {
        String userId = null;
        byte[] userIdBytes = message.getUserId();

        if (userIdBytes != null) {
            userId = new String(userIdBytes, UTF8);
        }

        return userId;
    }

    @Override
    public void setUserId(String userId) {
        byte[] bytes = null;
        if (userId != null) {
            bytes = userId.getBytes(UTF8);
        }

        if (bytes == null) {
            if (message.getProperties() != null) {
                message.getProperties().setUserId(null);
            }
        } else {
            message.setUserId(bytes);
        }
    }

    @Override
    public byte[] getUserIdBytes() {
        return message.getUserId();
    }

    @Override
    public void setUserIdBytes(byte[] userId) {
        if (userId == null || userId.length == 0) {
            if (message.getProperties() != null) {
                message.getProperties().setUserId(null);
            }
        } else {
            message.setUserId(userId);
        }
    }

    @Override
    public String getGroupId() {
        return message.getGroupId();
    }

    @Override
    public void setGroupId(String groupId) {
        message.setGroupId(groupId);
    }

    @Override
    public int getGroupSequence() {
        if (message.getProperties() != null) {
            UnsignedInteger groupSeqUint = message.getProperties().getGroupSequence();
            if (groupSeqUint != null) {
                // This wraps it into the negative int range if uint is over 2^31-1
                return groupSeqUint.intValue();
            }
        }

        return 0;
    }

    @Override
    public void setGroupSequence(int groupSequence) {
        // This wraps it into the upper uint range if a negative was provided
        if (groupSequence == 0) {
            if (message.getProperties() != null) {
                message.getProperties().setGroupSequence(null);
            }
        } else {
            message.setGroupSequence(groupSequence);
        }
    }

    /**
     * @return the true AMQP Message instance wrapped by this Facade.
     */
    public Message getAmqpMessage() {
        return this.message;
    }

    /**
     * The AmqpConnection instance that is associated with this Message.
     * @return the connection
     */
    public AmqpConnection getConnection() {
        return connection;
    }

    /**
     * Checks for the presence of a given message annotation and returns true
     * if it is contained in the current annotations.  If the annotations have
     * not yet been initialized then this method always returns false.
     *
     * @param key
     *        the name of the annotation to query for.
     *
     * @return true if the annotation is present, false in not or annotations not initialized.
     */
    boolean messageAnnotationExists(String key) {
        if (messageAnnotationsMap == null) {
            return false;
        }

        return messageAnnotationsMap.containsKey(AmqpMessageSupport.getSymbol(key));
    }

    /**
     * Given a message annotation name, lookup and return the value associated with
     * that annotation name.  If the message annotations have not been created yet
     * then this method will always return null.
     *
     * @param key
     *        the Symbol name that should be looked up in the message annotations.
     *
     * @return the value of the annotation if it exists, or null if not set or not accessible.
     */
    Object getMessageAnnotation(String key) {
        if (messageAnnotationsMap == null) {
            return null;
        }

        return messageAnnotationsMap.get(AmqpMessageSupport.getSymbol(key));
    }

    /**
     * Removes a message annotation if the message contains it.  Will not do
     * a lazy create on the message annotations so caller cannot count on the
     * existence of the message annotations after a call to this method.
     *
     * @param key
     *        the annotation key that is to be removed from the current set.
     */
    void removeMessageAnnotation(String key) {
        if (messageAnnotationsMap == null) {
            return;
        }

        messageAnnotationsMap.remove(AmqpMessageSupport.getSymbol(key));
    }

    /**
     * Perform a proper annotation set on the AMQP Message based on a Symbol key and
     * the target value to append to the current annotations.
     *
     * @param key
     *        The name of the Symbol whose value is being set.
     * @param value
     *        The new value to set in the annotations of this message.
     */
    void setMessageAnnotation(String key, Object value) {
        lazyCreateMessageAnnotations();
        messageAnnotationsMap.put(AmqpMessageSupport.getSymbol(key), value);
    }

    /**
     * Removes all message annotations from this message.
     */
    void clearMessageAnnotations() {
        messageAnnotationsMap = null;
        message.setMessageAnnotations(null);
    }

    /**
     * Removes all application level properties from the Message.
     */
    void clearAllApplicationProperties() {
        applicationPropertiesMap = null;
        message.setApplicationProperties(null);
    }

    String getToAddress() {
        return message.getAddress();
    }

    void setToAddress(String address) {
        message.setAddress(address);
    }

    String getReplyToAddress() {
        return message.getReplyTo();
    }

    void setReplyToAddress(String address) {
        this.message.setReplyTo(address);
    }

    private Long getAbsoluteExpiryTime() {
        Long result = null;
        if (message.getProperties() != null) {
            Date date = message.getProperties().getAbsoluteExpiryTime();
            if (date != null) {
                result = date.getTime();
            }
        }

        return result;
    }

    private Long getTtl() {
        Long result = null;
        if (message.getHeader() != null) {
            UnsignedInteger ttl = message.getHeader().getTtl();
            if (ttl != null) {
                result = ttl.longValue();
            }
        }

        return result;
    }

    private void setAbsoluteExpiryTime(Long expiration) {
        if (expiration == null) {
            if (message.getProperties() != null) {
                message.getProperties().setAbsoluteExpiryTime(null);
            }
        } else {
            message.setExpiryTime(expiration);
        }
    }

    private void lazyCreateMessageAnnotations() {
        if (messageAnnotationsMap == null) {
            messageAnnotationsMap = new HashMap<Symbol,Object>();
            message.setMessageAnnotations(new MessageAnnotations(messageAnnotationsMap));
        }
    }

    private void lazyCreateApplicationProperties() {
        if (applicationPropertiesMap == null) {
            applicationPropertiesMap = new HashMap<String, Object>();
            message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));
        }
    }
}
