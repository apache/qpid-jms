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
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_DELIVERY_TIME;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.MessageFormatException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.exceptions.IdConversionException;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.AmqpConnection;
import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;

import io.netty.buffer.ByteBuf;

public class AmqpJmsMessageFacade implements JmsMessageFacade {

    private static final long UINT_MAX = 0xFFFFFFFFL;

    protected AmqpConnection connection;

    private Properties properties;
    private final AmqpHeader header = new AmqpHeader();
    private Section body;
    private Map<Symbol, Object> messageAnnotationsMap;
    private Map<String, Object> applicationPropertiesMap;
    private Map<Symbol, Object> deliveryAnnotationsMap;
    private Map<Symbol, Object> footerMap;

    private JmsDestination replyTo;
    private JmsDestination destination;
    private JmsDestination consumerDestination;

    private Long syntheticExpiration;
    private long syntheticDeliveryTime;

    /**
     * Used to record the value of JMS_AMQP_TTL property
     * if it is explicitly set by the application
     */
    private Long userSpecifiedTTL = null;

    /**
     * Initialize the state of this message for send.
     *
     * @param connection
     *      The connection that this message is linked to.
     */
    public void initialize(AmqpConnection connection) {
        this.connection = connection;

        setPersistent(true);
        initializeEmptyBody();
    }

    /**
     * Initialize the state of this message for receive.
     *
     * @param consumer
     *      The consumer that this message was read from.
     */
    public void initialize(AmqpConsumer consumer) {
        this.connection = consumer.getConnection();
        this.consumerDestination = consumer.getDestination();

        Long ttl = getTtl();
        Long absoluteExpiryTime = getAbsoluteExpiryTime();
        if (absoluteExpiryTime == null && ttl != null) {
            syntheticExpiration = System.currentTimeMillis() + ttl;
        }

        if (getMessageAnnotation(JMS_DELIVERY_TIME) == null) {
            syntheticDeliveryTime = getTimestamp();
        }

        // We now know what type of message this is, so remove this so if resent the
        // annotations can come from the cache if possible.
        removeMessageAnnotation(AmqpMessageSupport.JMS_MSG_TYPE);
    }

    /**
     * Used to indicate that a Message object should empty the body element and make
     * any other internal updates to reflect the message now has no body value.
     */
    protected void initializeEmptyBody() {
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
     * @return a Symbol value indicating the message content type.
     */
    public Symbol getContentType() {
        if (properties != null && properties.getContentType() != null) {
            return properties.getContentType();
        }

        return null;
    }

    public void setContentType(Symbol value) {
        if (properties == null) {
            if (value == null) {
                return;
            }
            lazyCreateProperties();
        }

        properties.setContentType(value);
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

        header.setTimeToLive(ttl);
    }

    @Override
    public void onDispatch() throws JMSException {
    }

    @Override
    public void clearBody() {
        setBody(null);
    }

    @Override
    public void clearProperties() throws JMSException {
        AmqpJmsMessagePropertyIntercepter.clearProperties(this);
    }

    @Override
    public AmqpJmsMessageFacade copy() throws JMSException {
        AmqpJmsMessageFacade copy = new AmqpJmsMessageFacade();
        copyInto(copy);
        return copy;
    }

    protected void copyInto(AmqpJmsMessageFacade target) {
        target.connection = connection;
        target.consumerDestination = consumerDestination;
        target.syntheticExpiration = syntheticExpiration;
        target.syntheticDeliveryTime = syntheticDeliveryTime;
        target.userSpecifiedTTL = userSpecifiedTTL;

        if (destination != null) {
            target.setDestination(destination);
        }

        if (replyTo != null) {
            target.setReplyTo(replyTo);
        }

        target.setAmqpHeader(header);

        if (properties != null) {
            target.setProperties(new Properties(properties));
        }

        target.setBody(body);

        if (deliveryAnnotationsMap != null && !deliveryAnnotationsMap.isEmpty()) {
            target.lazyCreateDeliveryAnnotations();
            target.deliveryAnnotationsMap.putAll(deliveryAnnotationsMap);
        }

        if (applicationPropertiesMap != null && !applicationPropertiesMap.isEmpty()) {
            target.lazyCreateApplicationProperties();
            target.applicationPropertiesMap.putAll(applicationPropertiesMap);
        }

        if (messageAnnotationsMap != null && !messageAnnotationsMap.isEmpty()) {
            target.lazyCreateMessageAnnotations();
            target.messageAnnotationsMap.putAll(messageAnnotationsMap);
        }

        if (footerMap != null && !footerMap.isEmpty()) {
            target.lazyCreateFooter();
            target.footerMap.putAll(footerMap);
        }
    }

    @Override
    public String getMessageId() {
        Object underlying = null;

        if (properties != null) {
            underlying = properties.getMessageId();
        }

        return AmqpMessageIdHelper.toMessageIdString(underlying);
    }

    @Override
    public Object getProviderMessageIdObject() {
        return properties == null ? null : properties.getMessageId();
    }

    @Override
    public void setProviderMessageIdObject(Object messageId) {
        if (properties == null) {
            if (messageId == null) {
                return;
            }

            lazyCreateProperties();
        }

        properties.setMessageId(messageId);
    }

    @Override
    public void setMessageId(String messageId) throws IdConversionException {
        Object value = AmqpMessageIdHelper.toIdObject(messageId);

        if (properties == null) {
            if (value == null) {
                return;
            }

            lazyCreateProperties();
        }

        properties.setMessageId(value);
    }

    @Override
    public long getTimestamp() {
        if (properties != null) {
            Date timestamp = properties.getCreationTime();
            if (timestamp != null) {
                return timestamp.getTime();
            }
        }

        return 0L;
    }

    @Override
    public void setTimestamp(long timestamp) {
        if (properties == null) {
            if (timestamp == 0) {
                return;
            }

            lazyCreateProperties();
        }

        if (timestamp == 0) {
            properties.setCreationTime(null);
        } else {
            properties.setCreationTime(new Date(timestamp));
        }
    }

    @Override
    public String getCorrelationId() {
        if (properties == null) {
            return null;
        }

        return AmqpMessageIdHelper.toCorrelationIdString(properties.getCorrelationId());
    }

    @Override
    public void setCorrelationId(String correlationId) throws IdConversionException {
        Object idObject = null;

        if (correlationId != null) {
            if (AmqpMessageIdHelper.hasMessageIdPrefix(correlationId)) {
                // JMSMessageID value, process it for possible type conversion
                idObject = AmqpMessageIdHelper.toIdObject(correlationId);
            } else {
                idObject = correlationId;
            }
        }

        if (properties == null) {
            if (idObject == null) {
                return;
            }

            lazyCreateProperties();
        }

        properties.setCorrelationId(idObject);
    }

    @Override
    public byte[] getCorrelationIdBytes() throws JMSException {
        Object correlationId = null;

        if (properties != null) {
            correlationId = properties.getCorrelationId();
        }

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

        if (properties == null) {
            if (binaryIdValue == null) {
                return;
            }

            lazyCreateProperties();
        }

        properties.setCorrelationId(binaryIdValue);
    }

    @Override
    public boolean isPersistent() {
        return header.isDurable();
    }

    @Override
    public void setPersistent(boolean value) {
        header.setDurable(value);
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
        return header.getDeliveryCount();
    }

    @Override
    public void setRedeliveryCount(int redeliveryCount) {
        header.setDeliveryCount(redeliveryCount);
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
        if (properties != null) {
            return properties.getSubject();
        }

        return null;
    }

    @Override
    public void setType(String type) {
        if (type != null) {
            lazyCreateProperties();
            properties.setSubject(type);
        } else {
            if (properties != null) {
                properties.setSubject(null);
            }
        }
    }

    @Override
    public int getPriority() {
        return header.getPriority();
    }

    @Override
    public void setPriority(int priority) {
        header.setPriority(priority);
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

    @Override
    public long getDeliveryTime() {
        Object deliveryTime = getMessageAnnotation(JMS_DELIVERY_TIME);
        if (deliveryTime instanceof Number) {
            return ((Number) deliveryTime).longValue();
        } else if (deliveryTime instanceof Date) {
            return ((Date) deliveryTime).getTime();
        } else if (deliveryTime != null) {
            throw new JMSRuntimeException("Unexpected delivery time annotation type: " + deliveryTime.getClass());
        }

        return syntheticDeliveryTime;
    }

    @Override
    public void setDeliveryTime(long deliveryTime, boolean transmit) {
        if (deliveryTime != 0 && transmit) {
            syntheticDeliveryTime = 0;
            setMessageAnnotation(JMS_DELIVERY_TIME, deliveryTime);
        } else {
            syntheticDeliveryTime = deliveryTime;
            removeMessageAnnotation(JMS_DELIVERY_TIME);
        }
    }

    @Override
    public boolean isDeliveryTimeTransmitted() {
        return getMessageAnnotation(JMS_DELIVERY_TIME) != null;
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
            this.destination = AmqpDestinationHelper.getJmsDestination(this, consumerDestination);
        }

        return destination;
    }

    @Override
    public void setDestination(JmsDestination destination) {
        this.destination = destination;
        AmqpDestinationHelper.setToAddressFromDestination(this, destination);
    }

    @Override
    public JmsDestination getReplyTo() {
        if (replyTo == null) {
            replyTo = AmqpDestinationHelper.getJmsReplyTo(this, consumerDestination);
        }

        return replyTo;
    }

    @Override
    public void setReplyTo(JmsDestination replyTo) {
        this.replyTo = replyTo;
        AmqpDestinationHelper.setReplyToAddressFromDestination(this, replyTo);
    }

    public void setReplyToGroupId(String replyToGroupId) {
        if (replyToGroupId != null) {
            lazyCreateProperties();
            properties.setReplyToGroupId(replyToGroupId);
        } else {
            if (properties != null) {
                properties.setReplyToGroupId(null);
            }
        }
    }

    public String getReplyToGroupId() {
        if (properties != null) {
            return properties.getReplyToGroupId();
        }

        return null;
    }

    @Override
    public String getUserId() {
        String userId = null;

        if (properties != null && properties.getUserId() != null) {
            Binary userIdBytes = properties.getUserId();
            if (userIdBytes.getLength() != 0) {
                userId = new String(userIdBytes.getArray(), userIdBytes.getArrayOffset(), userIdBytes.getLength(), StandardCharsets.UTF_8);
            }
        }

        return userId;
    }

    @Override
    public void setUserId(String userId) {
        byte[] bytes = null;
        if (userId != null) {
            bytes = userId.getBytes(StandardCharsets.UTF_8);
        }

        if (bytes == null) {
            if (properties != null) {
                properties.setUserId(null);
            }
        } else {
            lazyCreateProperties();
            properties.setUserId(new Binary(bytes));
        }
    }

    @Override
    public byte[] getUserIdBytes() {
        if(properties == null || properties.getUserId() == null) {
            return null;
        } else {
            final Binary userId = properties.getUserId();
            byte[] id = new byte[userId.getLength()];
            System.arraycopy(userId.getArray(), userId.getArrayOffset(), id, 0, userId.getLength());
            return id;
        }
    }

    @Override
    public void setUserIdBytes(byte[] userId) {
        if (userId == null || userId.length == 0) {
            if (properties != null) {
                properties.setUserId(null);
            }
        } else {
            lazyCreateProperties();
            byte[] id = new byte[userId.length];
            System.arraycopy(userId, 0, id, 0, userId.length);
            properties.setUserId(new Binary(id));
        }
    }

    @Override
    public String getGroupId() {
        if (properties != null) {
            return properties.getGroupId();
        }

        return null;
    }

    @Override
    public void setGroupId(String groupId) {
        if (groupId != null) {
            lazyCreateProperties();
            properties.setGroupId(groupId);
        } else {
            if (properties != null) {
                properties.setGroupId(null);
            }
        }
    }

    @Override
    public int getGroupSequence() {
        if (properties != null) {
            UnsignedInteger groupSeqUint = properties.getGroupSequence();
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
        if (groupSequence != 0) {
            lazyCreateProperties();
            properties.setGroupSequence(UnsignedInteger.valueOf(groupSequence));
        } else {
            if (properties != null) {
                properties.setGroupSequence(null);
            }
        }
    }

    @Override
    public boolean hasBody() {
        return body != null;
    }

    /**
     * The AmqpConnection instance that is associated with this Message.
     * @return the connection
     */
    AmqpConnection getConnection() {
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
    boolean messageAnnotationExists(Symbol key) {
        if (messageAnnotationsMap == null) {
            return false;
        }

        return messageAnnotationsMap.containsKey(key);
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
    Object getMessageAnnotation(Symbol key) {
        if (messageAnnotationsMap == null) {
            return null;
        }

        return messageAnnotationsMap.get(key);
    }

    /**
     * Removes a message annotation if the message contains it.  Will not do
     * a lazy create on the message annotations so caller cannot count on the
     * existence of the message annotations after a call to this method.
     *
     * @param key
     *        the annotation key that is to be removed from the current set.
     */
    void removeMessageAnnotation(Symbol key) {
        if (messageAnnotationsMap == null) {
            return;
        }

        messageAnnotationsMap.remove(key);
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
    void setMessageAnnotation(Symbol key, Object value) {
        lazyCreateMessageAnnotations();
        messageAnnotationsMap.put(key, value);
    }

    /**
     * Removes all message annotations from this message.
     */
    void clearMessageAnnotations() {
        messageAnnotationsMap = null;
    }

    /**
     * Removes all application level properties from the Message.
     */
    void clearAllApplicationProperties() {
        applicationPropertiesMap = null;
    }

    String getToAddress() {
        if (properties != null) {
            return properties.getTo();
        }

        return null;
    }

    void setToAddress(String address) {
        if (address != null) {
            lazyCreateProperties();
            properties.setTo(address);
        } else {
            if (properties != null) {
                properties.setTo(null);
            }
        }
    }

    String getReplyToAddress() {
        if (properties != null) {
            return properties.getReplyTo();
        }

        return null;
    }

    void setReplyToAddress(String address) {
        if (address != null) {
            lazyCreateProperties();
            properties.setReplyTo(address);
        } else {
            if (properties != null) {
                properties.setReplyTo(null);
            }
        }
    }

    JmsDestination getConsumerDestination() {
        return this.consumerDestination;
    }

    public JmsMessage asJmsMessage() {
        return new JmsMessage(this);
    }

    @Override
    public ByteBuf encodeMessage() {
        return AmqpCodec.encodeMessage(this);
    }

    //----- Access to AMQP Message Values ------------------------------------//

    AmqpHeader getAmqpHeader() {
        return header;
    }

    void setAmqpHeader(AmqpHeader header) {
        this.header.setHeader(header);
    }

    Header getHeader() {
        return header.getHeader();
    }

    void setHeader(Header header) {
        this.header.setHeader(header);
    }

    Properties getProperties() {
        return properties;
    }

    void setProperties(Properties properties) {
        this.properties = properties;
    }

    Section getBody() {
        return body;
    }

    void setBody(Section body) {
        this.body = body;
    }

    MessageAnnotations getMessageAnnotations() {
        MessageAnnotations result = null;
        if (messageAnnotationsMap != null && !messageAnnotationsMap.isEmpty()) {
            result = new MessageAnnotations(messageAnnotationsMap);
        }

        return result;
    }

    void setMessageAnnotations(MessageAnnotations messageAnnotations) {
        if (messageAnnotations != null) {
            this.messageAnnotationsMap = messageAnnotations.getValue();
        }
    }

    DeliveryAnnotations getDeliveryAnnotations() {
        DeliveryAnnotations result = null;
        if (deliveryAnnotationsMap != null && !deliveryAnnotationsMap.isEmpty()) {
            result = new DeliveryAnnotations(deliveryAnnotationsMap);
        }

        return result;
    }

    void setDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        if (deliveryAnnotations != null) {
            this.deliveryAnnotationsMap = deliveryAnnotations.getValue();
        }
    }

    ApplicationProperties getApplicationProperties() {
        ApplicationProperties result = null;
        if (applicationPropertiesMap != null && !applicationPropertiesMap.isEmpty()) {
            result = new ApplicationProperties(applicationPropertiesMap);
        }
        return result;
    }

    void setApplicationProperties(ApplicationProperties applicationProperties) {
        if (applicationProperties != null) {
            this.applicationPropertiesMap = applicationProperties.getValue();
        }
    }

    Footer getFooter() {
        Footer result = null;
        if (footerMap != null && !footerMap.isEmpty()) {
            result = new Footer(footerMap);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    void setFooter(Footer footer) {
        if (footer != null) {
            this.footerMap = footer.getValue();
        }
    }

    //----- Internal Message Utility Methods ---------------------------------//

    private Long getAbsoluteExpiryTime() {
        Long result = null;
        if (properties != null) {
            Date date = properties.getAbsoluteExpiryTime();
            if (date != null) {
                result = date.getTime();
            }
        }

        return result;
    }

    private Long getTtl() {
        Long result = null;
        if (header.nonDefaultTimeToLive()) {
            result = header.getTimeToLive();
        }

        return result;
    }

    private void setAbsoluteExpiryTime(Long expiration) {
        if (expiration == null || expiration == 0l) {
            if (properties != null) {
                properties.setAbsoluteExpiryTime(null);
            }
        } else {
            lazyCreateProperties();
            properties.setAbsoluteExpiryTime(new Date(expiration));
        }
    }

    private void lazyCreateProperties() {
        if (properties == null) {
            properties = new Properties();
        }
    }

    private void lazyCreateMessageAnnotations() {
        if (messageAnnotationsMap == null) {
            messageAnnotationsMap = new HashMap<Symbol, Object>();
        }
    }

    private void lazyCreateDeliveryAnnotations() {
        if (deliveryAnnotationsMap == null) {
            deliveryAnnotationsMap = new HashMap<Symbol, Object>();
        }
    }

    private void lazyCreateApplicationProperties() {
        if (applicationPropertiesMap == null) {
            applicationPropertiesMap = new HashMap<String, Object>();
        }
    }

    private void lazyCreateFooter() {
        if (footerMap == null) {
            footerMap = new HashMap<Symbol, Object>();
        }
    }
}
