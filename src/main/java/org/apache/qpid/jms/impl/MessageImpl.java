/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.jms.impl;

import static org.apache.qpid.jms.impl.ClientProperties.JMS_AMQP_TTL;
import static org.apache.qpid.jms.impl.ClientProperties.JMS_AMQP_REPLY_TO_GROUP_ID;
import static org.apache.qpid.jms.impl.ClientProperties.JMSXUSERID;
import static org.apache.qpid.jms.impl.ClientProperties.JMSXGROUPID;
import static org.apache.qpid.jms.impl.ClientProperties.JMSXGROUPSEQ;
import static org.apache.qpid.jms.impl.MessageIdHelper.JMS_ID_PREFIX;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.qpid.jms.engine.AmqpMessage;

public abstract class MessageImpl<T extends AmqpMessage> implements Message
{
    private static final int JMS_MAX_PRIORITY = 9;
    private static final long MAX_UINT = 0xFFFFFFFFL;
    private final T _amqpMessage;
    private final SessionImpl _sessionImpl;
    private Long _jmsExpirationFromTTL = null;
    private Destination _destination;
    private Destination _replyTo;
    private boolean _propertiesWritable;

    /**
     * Used to record the value of JMS_AMQP_TTL property
     * if it is explicitly set by the application
     */
    private Long _propJMS_AMQP_TTL = null;

    //message to be sent
    public MessageImpl(T amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl)
    {
        _amqpMessage = amqpMessage;
        _sessionImpl = sessionImpl;
        _propertiesWritable = true;
    }

    //message just received
    public MessageImpl(T amqpMessage, SessionImpl sessionImpl, ConnectionImpl connectionImpl, Destination consumerDestination)
    {
        _amqpMessage = amqpMessage;
        _sessionImpl = sessionImpl;

        String to = _amqpMessage.getTo();
        String toTypeString = (String) _amqpMessage.getMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        _destination = sessionImpl.getDestinationHelper().decodeDestination(to, toTypeString, consumerDestination, false);

        String replyTo = _amqpMessage.getReplyTo();
        String replyToTypeString = (String) _amqpMessage.getMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        _replyTo = sessionImpl.getDestinationHelper().decodeDestination(replyTo, replyToTypeString, consumerDestination, true);

        //If we have to synthesize JMSExpiration from only the AMQP TTL header, calculate it now
        Long ttl = _amqpMessage.getTtl();
        Long absoluteExpiryTime = _amqpMessage.getAbsoluteExpiryTime();
        if(absoluteExpiryTime == null && ttl != null)
        {
            _jmsExpirationFromTTL = System.currentTimeMillis() + ttl;
        }

        _propertiesWritable = false;
    }

    T getUnderlyingAmqpMessage(boolean prepareForSending)
    {
        if(prepareForSending)
        {
            return prepareUnderlyingAmqpMessageForSending(_amqpMessage);
        }
        else
        {
            return _amqpMessage;
        }
    }

    protected abstract T prepareUnderlyingAmqpMessageForSending(T amqpMessage);

    private void checkPropertyNameIsValid(String propertyName) throws IllegalArgumentException
    {
        if (propertyName == null)
        {
            throw new IllegalArgumentException("Property name must not be null");
        }
        else if (propertyName.length() == 0)
        {
            throw new IllegalArgumentException("Property name must not be the empty string");
        }

        checkIdentifierFormat(propertyName);
    }

    private void checkIdentifierFormat(String identifier) throws IllegalArgumentException
    {
        checkIdentifierLetterAndDigitRequirements(identifier);
        checkIdentifierIsntNullTrueFalse(identifier);
        checkIdentifierIsntLogicOperator(identifier);
    }

    private void checkIdentifierIsntLogicOperator(String identifier)
    {
        //Identifiers cannot be NOT, AND, OR, BETWEEN, LIKE, IN, IS, or ESCAPE.
        if("NOT".equals(identifier) || "AND".equals(identifier) || "OR".equals(identifier)
                || "BETWEEN".equals(identifier) || "LIKE".equals(identifier) || "IN".equals(identifier)
                    || "IS".equals(identifier) || "ESCAPE".equals(identifier))
        {
            throw new IllegalArgumentException("Identifier not allowed in JMS: '" + identifier + "'");
        }
    }

    private void checkIdentifierIsntNullTrueFalse(String identifier)
    {
        //Identifiers cannot be the names NULL, TRUE, and FALSE.
        if("NULL".equals(identifier) || "TRUE".equals(identifier) || "FALSE".equals(identifier))
        {
            throw new IllegalArgumentException("Identifier not allowed in JMS: '" + identifier + "'");
        }
    }

    private void checkIdentifierLetterAndDigitRequirements(String identifier)
    {
        //An identifier is an unlimited-length sequence of letters and digits, the first of which must be a letter.
        //A letter is any character for which the method Character.isJavaLetter returns true. This includes '_' and '$'.
        //A letter or digit is any character for which the method Character.isJavaLetterOrDigit returns true.
        char startChar = identifier.charAt(0);
        if (!(Character.isJavaIdentifierStart(startChar)))
        {
            throw new IllegalArgumentException("Identifier does not begin with a valid JMS identifier start character: '" + identifier + "' ");
        }

        // JMS part character
        int length = identifier.length();
        for (int i = 1; i < length; i++)
        {
            char ch = identifier.charAt(i);
            if (!(Character.isJavaIdentifierPart(ch)))
            {
                throw new IllegalArgumentException("Identifier contains invalid JMS identifier character '" + ch + "': '" + identifier + "' ");
            }
        }
    }

    private void setPropertiesWritable(boolean writable)
    {
        _propertiesWritable = writable;
    }

    private void checkPropertiesWritable() throws MessageNotWriteableException
    {
        if(!_propertiesWritable)
        {
            throw new MessageNotWriteableException("Message properties are currently in read-only mode");
        }
    }

    private boolean checkObjectPropertyValueIsValid(Object object) throws MessageFormatException
    {
        boolean valid = object instanceof Boolean || object instanceof Byte || object instanceof Short ||
                        object instanceof Integer || object instanceof Long || object instanceof Float ||
                        object instanceof Double || object instanceof String|| object == null;
        if(!valid)
        {
            throw createMessageFormatException("Invalid object property value type: " + object.getClass());
        }

        return true;
    }

    private void setApplicationProperty(String name, Object value) throws MessageFormatException, MessageNotWriteableException
    {
        checkPropertiesWritable();
        checkPropertyNameIsValid(name);

        if(JMS_AMQP_TTL.equals(name))
        {
            setJMS_AMQP_TTL(value);
            return;
        }
        else if(JMS_AMQP_REPLY_TO_GROUP_ID.equals(name))
        {
            setJMS_AMQP_REPLY_TO_GROUP_ID(value);
            return;
        }
        else if(JMSXUSERID.equals(name))
        {
            setJMSXUserID(value);
            return;
        }
        else if(JMSXGROUPID.equals(name))
        {
            setJMSXGroupID(value);
            return;
        }
        else if(JMSXGROUPSEQ.equals(name))
        {
            setJMSXGroupSeq(value);
            return;
        }

        checkObjectPropertyValueIsValid(value);

        _amqpMessage.setApplicationProperty(name, value);
    }

    private void setJMSXGroupID(Object value) throws MessageFormatException
    {
        String groupId = null;
        if(value != null)
        {
            if(value instanceof String)
            {
                groupId = (String) value;
            }
            else
            {
                throw createMessageFormatException(JMSXGROUPID + " must be a String");
            }
        }

        _amqpMessage.setGroupId(groupId);
    }

    private void setJMSXGroupSeq(Object value) throws MessageFormatException
    {
        Long groupSeq = null;
        if(value != null)
        {
            if(value instanceof Integer)
            {
                groupSeq = ((Integer) value).longValue();
            }
            else
            {
                throw createMessageFormatException(JMSXGROUPID + " must be an Integer");
            }
        }

        _amqpMessage.setGroupSequence(groupSeq);
    }

    private void setJMSXUserID(Object value) throws MessageFormatException
    {
        byte[] userIdBytes = null;
        if(value != null)
        {
            if(value instanceof String)
            {
                try
                {
                    userIdBytes = ((String) value).getBytes("UTF-8");
                }
                catch (UnsupportedEncodingException e)
                {
                    throw createMessageFormatException("Unable to encode user id", e);
                }
            }
            else
            {
                throw createMessageFormatException(JMSXUSERID + " must be a String");
            }
        }

        _amqpMessage.setUserId(userIdBytes);
    }

    private void setJMS_AMQP_TTL(Object value) throws MessageFormatException
    {
        Long ttl = null;
        if(value instanceof Long)
        {
            ttl = (Long) value;
        }

        if(ttl != null && ttl >= 0 && ttl <= MAX_UINT)
        {
            _propJMS_AMQP_TTL = ttl;
        }
        else
        {
            throw createMessageFormatException(JMS_AMQP_TTL + " must be a long with value in range 0 to 2^31 - 1");
        }
    }

    private void setJMS_AMQP_REPLY_TO_GROUP_ID(Object value) throws MessageFormatException
    {
        String replyToGroupId = null;
        if(value != null)
        {
            if(value instanceof String)
            {
                replyToGroupId = (String) value;
            }
            else
            {
                throw createMessageFormatException(JMS_AMQP_REPLY_TO_GROUP_ID + " must be a String");
            }
        }

        _amqpMessage.setReplyToGroupId(replyToGroupId);
    }

    private Object getApplicationProperty(String name) throws MessageFormatException
    {
        checkPropertyNameIsValid(name);

        if(JMS_AMQP_TTL.equals(name))
        {
            return _propJMS_AMQP_TTL;
        }
        else if(JMSXUSERID.equals(name))
        {
            return getJMSXUserID();
        }
        else if(JMSXGROUPID.equals(name))
        {
            return _amqpMessage.getGroupId();
        }
        else if(JMSXGROUPSEQ.equals(name))
        {
            return getJMSXGroupSeq();
        }
        else if(JMS_AMQP_REPLY_TO_GROUP_ID.equals(name))
        {
            return _amqpMessage.getReplyToGroupId();
        }

        //TODO: handle non-JMS types?
        return _amqpMessage.getApplicationProperty(name);
    }

    private String getJMSXUserID() throws MessageFormatException
    {
        byte[] userId = _amqpMessage.getUserId();
        if(userId == null)
        {
            return null;
        }
        else
        {
            try
            {
                return new String(userId, "UTF-8");
            }
            catch (UnsupportedEncodingException e)
            {
                throw createMessageFormatException("Unable to decode user id", e);
            }
        }
    }

    private Integer getJMSXGroupSeq()
    {
        Long groupSeqUint = _amqpMessage.getGroupSequence();
        if(groupSeqUint == null)
        {
            return null;
        }
        else
        {
            //The long represents a uint, so may be 0 to 2^32-1.
            //This wraps it into a negative int range if over 2^31-1
            return groupSeqUint.intValue();
        }
    }

    private MessageFormatException createMessageFormatException(String message)
    {
        return createMessageFormatException(message, null);
    }

    private MessageFormatException createMessageFormatException(String message, Exception cause)
    {
        MessageFormatException mfe = new MessageFormatException(message);
        if(cause != null)
        {
            mfe.setLinkedException(cause);
            mfe.initCause(cause);
        }

        return mfe;
    }

    private boolean propertyExistsJMSXUserID()
    {
        return _amqpMessage.getUserId() != null;
    }

    private boolean propertyExistsJMSXGroupID()
    {
        return _amqpMessage.getGroupId() != null;
    }

    private boolean propertyExistsJMSXGroupSeq()
    {
        return _amqpMessage.getGroupSequence() != null;
    }

    private boolean propertyExistsJMS_AMQP_TTL()
    {
        return _propJMS_AMQP_TTL != null;
    }

    private boolean propertyExistsJMS_AMQP_REPLY_TO_GROUP_ID()
    {
        return _amqpMessage.getReplyToGroupId() != null;
    }

    //======= JMS Methods =======


    @Override
    public String getJMSMessageID() throws JMSException
    {
        MessageIdHelper messageIdHelper = _sessionImpl.getMessageIdHelper();

        String baseIdString = messageIdHelper.toBaseMessageIdString(_amqpMessage.getMessageId());

        if(baseIdString == null)
        {
            return null;
        }
        else
        {
            return JMS_ID_PREFIX + baseIdString;
        }
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException
    {
        MessageIdHelper messageIdHelper = _sessionImpl.getMessageIdHelper();

        //We permit null, but otherwise ensure that the value starts "ID:"
        if(id != null && !messageIdHelper.hasMessageIdPrefix(id))
        {
            throw new QpidJmsException("Provided JMSMessageID does not have required '" + JMS_ID_PREFIX + "' prefix");
        }

        String stripped = messageIdHelper.stripMessageIdPrefix(id);

        _amqpMessage.setMessageId(stripped);
    }

    @Override
    public long getJMSTimestamp() throws JMSException
    {
        Long timestamp = _amqpMessage.getCreationTime();
        if(timestamp == null)
        {
            return 0;
        }
        else
        {
            return timestamp.longValue();
        }
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException
    {
        if(timestamp != 0)
        {
            _amqpMessage.setCreationTime(timestamp);
        }
        else
        {
            _amqpMessage.setCreationTime(null);
        }
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException
    {
        Object correlationId = _amqpMessage.getCorrelationId();
        if(correlationId == null)
        {
            return null;
        }
        else if(correlationId instanceof ByteBuffer)
        {
            ByteBuffer dup = ((ByteBuffer) correlationId).duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);

            return bytes;
        }
        else
        {
            throw new QpidJmsException("The underlying correlation-id is not binary and so can't be returned");
        }
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
    {
        if(correlationID == null)
        {
            _amqpMessage.setCorrelationId(correlationID);
        }
        else
        {
            byte[] bytes = Arrays.copyOf(correlationID, correlationID.length);
            _amqpMessage.setCorrelationId(ByteBuffer.wrap(bytes));
        }
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException
    {
        MessageIdHelper messageIdHelper = _sessionImpl.getMessageIdHelper();

        boolean appSpecific = false;
        boolean hasMessageIdPrefix = messageIdHelper.hasMessageIdPrefix(correlationID);
        if(correlationID != null && !hasMessageIdPrefix)
        {
            appSpecific = true;
        }

        String stripped = messageIdHelper.stripMessageIdPrefix(correlationID);

        if(hasMessageIdPrefix)
        {
            Object idObject = messageIdHelper.toIdObject(stripped);
            _amqpMessage.setCorrelationId(idObject);
        }
        else
        {
            _amqpMessage.setCorrelationId(stripped);
        }

        if(appSpecific)
        {
            _amqpMessage.setMessageAnnotation(ClientProperties.X_OPT_APP_CORRELATION_ID, true);
        }
        else
        {
            _amqpMessage.clearMessageAnnotation(ClientProperties.X_OPT_APP_CORRELATION_ID);
        }
    }

    @Override
    public String getJMSCorrelationID() throws JMSException
    {
        MessageIdHelper messageIdHelper = _sessionImpl.getMessageIdHelper();

        String baseIdString = messageIdHelper.toBaseMessageIdString(_amqpMessage.getCorrelationId());

        if(baseIdString == null)
        {
            return null;
        }
        else
        {
            boolean appSpecific = false;
            if(_amqpMessage.messageAnnotationExists(ClientProperties.X_OPT_APP_CORRELATION_ID))
            {
                appSpecific = (Boolean) _amqpMessage.getMessageAnnotation(ClientProperties.X_OPT_APP_CORRELATION_ID);
            }

            if(appSpecific)
            {
                return baseIdString;
            }
            else
            {
                return JMS_ID_PREFIX + baseIdString;
            }
        }
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException
    {
        return _replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException
    {
        _replyTo = replyTo;

        String replyToAddress = _sessionImpl.getDestinationHelper().decodeAddress(_replyTo);
        String typeString = _sessionImpl.getDestinationHelper().decodeTypeString(_replyTo);

        _amqpMessage.setReplyTo(replyToAddress);

        if(replyToAddress == null || typeString == null)
        {
            _amqpMessage.clearMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        }
        else
        {
            _amqpMessage.setMessageAnnotation(DestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeString);
        }
    }

    @Override
    public Destination getJMSDestination() throws JMSException
    {
        return _destination;
    }

    @Override
    public void setJMSDestination(final Destination destination) throws JMSException
    {
        _destination = destination;

        String to = _sessionImpl.getDestinationHelper().decodeAddress(destination);
        String typeString = _sessionImpl.getDestinationHelper().decodeTypeString(destination);

        _amqpMessage.setTo(to);

        if(to == null || typeString == null)
        {
            _amqpMessage.clearMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME);
        }
        else
        {
            _amqpMessage.setMessageAnnotation(DestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME, typeString);
        }
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException
    {
        if(getUnderlyingAmqpMessage(false).isDurable())
        {
            return DeliveryMode.PERSISTENT;
        }
        else
        {
            return DeliveryMode.NON_PERSISTENT;
        }
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException
    {
        if(DeliveryMode.PERSISTENT == deliveryMode)
        {
            getUnderlyingAmqpMessage(false).setDurable(true);
        }
        else
        {
            getUnderlyingAmqpMessage(false).setDurable(false);
        }
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public String getJMSType() throws JMSException
    {
        return (String) _amqpMessage.getMessageAnnotation(ClientProperties.X_OPT_JMS_TYPE);
    }

    @Override
    public void setJMSType(String type) throws JMSException
    {
        _amqpMessage.setMessageAnnotation(ClientProperties.X_OPT_JMS_TYPE, type);
    }

    @Override
    public long getJMSExpiration() throws JMSException
    {
        //Use absolute-expiry if present
        Long absoluteExpiry = _amqpMessage.getAbsoluteExpiryTime();
        if(absoluteExpiry != null)
        {
            return absoluteExpiry;
        }

        if(_jmsExpirationFromTTL != null)
        {
            return _jmsExpirationFromTTL;
        }

        //failing the above, there is no expiration
        return 0;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException
    {
        //clear the ttl-derived value in case it was set, we are changing to an explicit value
        _jmsExpirationFromTTL = null;

        if(expiration != 0)
        {
            _amqpMessage.setAbsoluteExpiryTime(expiration);
        }
        else
        {
            _amqpMessage.setAbsoluteExpiryTime(null);
        }
    }

    @Override
    public int getJMSPriority() throws JMSException
    {
        short priority = _amqpMessage.getPriority();

        if(priority > JMS_MAX_PRIORITY)
        {
            return JMS_MAX_PRIORITY;
        }
        else
        {
            return priority;
        }
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException
    {
        Integer i = priority;

        _amqpMessage.setPriority(i.shortValue());
    }

    @Override
    public void clearProperties() throws JMSException
    {
        setPropertiesWritable(true);

        _amqpMessage.clearAllApplicationProperties();
        _propJMS_AMQP_TTL = null;
        _amqpMessage.setReplyToGroupId(null);
        _amqpMessage.setUserId(null);
        _amqpMessage.setGroupId(null);
        _amqpMessage.setGroupSequence(null);

        //TODO: Clear any new custom properties.
    }

    @Override
    public boolean propertyExists(String name) throws JMSException
    {
        if(JMS_AMQP_TTL.equals(name))
        {
            return propertyExistsJMS_AMQP_TTL();
        }

        if(JMS_AMQP_REPLY_TO_GROUP_ID.equals(name))
        {
            return propertyExistsJMS_AMQP_REPLY_TO_GROUP_ID();
        }

        if(JMSXUSERID.equals(name))
        {
            return propertyExistsJMSXUserID();
        }

        if(JMSXGROUPID.equals(name))
        {
            return propertyExistsJMSXGroupID();
        }

        if(JMSXGROUPSEQ.equals(name))
        {
            return propertyExistsJMSXGroupSeq();
        }

        return _amqpMessage.applicationPropertyExists(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Boolean)
        {
            return ((Boolean) value).booleanValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Boolean.valueOf((String) value);
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to boolean.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public byte getByteProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Byte)
        {
            return ((Byte) value).byteValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Byte.valueOf((String) value).byteValue();
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to byte.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public short getShortProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Short)
        {
            return ((Short) value).shortValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).shortValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Short.valueOf((String) value).shortValue();
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to short.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public int getIntProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Integer)
        {
            return ((Integer) value).intValue();
        }
        else if (value instanceof Short)
        {
            return ((Short) value).intValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).intValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Integer.valueOf((String) value).intValue();
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to int.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public long getLongProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Long)
        {
            return ((Long) value).longValue();
        }
        else if (value instanceof Integer)
        {
            return ((Integer) value).longValue();
        }
        else if (value instanceof Short)
        {
            return ((Short) value).longValue();
        }
        else if (value instanceof Byte)
        {
            return ((Byte) value).longValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Long.valueOf((String) value).longValue();
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to long.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public float getFloatProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Float)
        {
            return ((Float) value).floatValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Float.valueOf((String) value).floatValue();
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to float.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if (value instanceof Double)
        {
            return ((Double) value).doubleValue();
        }
        else if (value instanceof Float)
        {
            return ((Float) value).doubleValue();
        }
        else if ((value instanceof String) || (value == null))
        {
            return Double.valueOf((String) value).doubleValue();
        }
        else
        {
            String message = "Property " + name + " of type " + value.getClass().getName() + " cannot be converted to double.";

            throw createMessageFormatException(message);
        }
    }

    @Override
    public String getStringProperty(String name) throws JMSException
    {
        Object value = getApplicationProperty(name);

        if ((value instanceof String) || (value == null))
        {
            return (String) value;
        }
        else
        {
            //TODO: verify it is a JMS type?
            return value.toString();
        }
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException
    {
        //TODO: verify it is a JMS type?
        return getApplicationProperty(name);
    }

    @Override
    public Enumeration<?> getPropertyNames() throws JMSException
    {
        //Get the base names from the underlying AMQP message
        Set<String> underlyingApplicationPropertyNames = _amqpMessage.getApplicationPropertyNames();

        //Create a new list we can mutate
        List<String> propNames = new ArrayList<String>(underlyingApplicationPropertyNames);

        if(propertyExistsJMS_AMQP_TTL())
        {
            propNames.add(JMS_AMQP_TTL);
        }

        if(propertyExistsJMS_AMQP_REPLY_TO_GROUP_ID())
        {
            propNames.add(JMS_AMQP_REPLY_TO_GROUP_ID);
        }

        if(propertyExistsJMSXUserID())
        {
            propNames.add(JMSXUSERID);
        }

        if(propertyExistsJMSXGroupID())
        {
            propNames.add(JMSXGROUPID);
        }

        if(propertyExistsJMSXGroupSeq())
        {
            propNames.add(JMSXGROUPSEQ);
        }

        return Collections.enumeration(propNames);
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException
    {
        setApplicationProperty(name, value);
    }

    @Override
    public void acknowledge() throws JMSException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public abstract void clearBody() throws JMSException;
}
