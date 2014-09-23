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
package org.apache.qpid.jms.message.facade.defaults;

import static org.fusesource.hawtbuf.Buffer.ascii;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsDestination;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.meta.JmsMessageId;
import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * A default implementation of the JmsMessageFaceade that provides a generic
 * message instance which can be used instead of implemented in Provider specific
 * version that maps to a Provider message object.
 */
public class JmsDefaultMessageFacade implements JmsMessageFacade {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    public static enum JmsMsgType {
        MESSAGE("jms/message"),
        BYTES("jms/bytes-message"),
        MAP("jms/map-message"),
        OBJECT("jms/object-message"),
        STREAM("jms/stream-message"),
        TEXT("jms/text-message"),
        TEXT_NULL("jms/text-message-null");

        public final AsciiBuffer buffer = new AsciiBuffer(this.name());
        public final AsciiBuffer mime;

        JmsMsgType(String mime) {
            this.mime = (ascii(mime));
        }
    }

    protected Map<String, Object> properties = new HashMap<String, Object>();

    protected byte priority = javax.jms.Message.DEFAULT_PRIORITY;
    protected String groupId;
    protected int groupSequence;
    protected JmsMessageId messageId;
    protected long expiration;
    protected long timestamp;
    protected String correlationId;
    protected boolean persistent;
    protected int redeliveryCount;
    protected String type;
    protected JmsDestination destination;
    protected JmsDestination replyTo;
    protected String userId;

    public JmsMsgType getMsgType() {
        return JmsMsgType.MESSAGE;
    }

    @Override
    public JmsDefaultMessageFacade copy() {
        JmsDefaultMessageFacade copy = new JmsDefaultMessageFacade();
        copyInto(copy);
        return copy;
    }

    protected void copyInto(JmsDefaultMessageFacade target) {
        target.priority = this.priority;
        target.groupSequence = this.groupSequence;
        target.groupId = this.groupId;
        target.expiration = this.expiration;
        target.timestamp = this.timestamp;
        target.correlationId = this.correlationId;
        target.persistent = this.persistent;
        target.redeliveryCount = this.redeliveryCount;
        target.type = this.type;
        target.destination = this.destination;
        target.replyTo = this.replyTo;
        target.userId = this.userId;

        if (this.messageId != null) {
            target.messageId = this.messageId.copy();
        }

        if (this.properties != null) {
            target.properties = new HashMap<String, Object>(this.properties);
        } else {
            target.properties = null;
        }
    }

    @Override
    public Map<String, Object> getProperties() throws JMSException {
        lazyCreateProperties();
        return properties;
    }

    @Override
    public boolean propertyExists(String key) throws JMSException {
        return this.properties.containsKey(key);
    }

    @Override
    public Object getProperty(String key) throws JMSException {
        return this.properties.get(key);
    }

    @Override
    public void setProperty(String key, Object value) throws JMSException {
        this.properties.put(key, value);
    }

    @Override
    public void onSend() throws JMSException {
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public void clearBody() {
    }

    @Override
    public void clearProperties() {
        properties.clear();
    }

    @Override
    public JmsMessageId getMessageId() {
        return this.messageId;
    }

    @Override
    public void setMessageId(JmsMessageId messageId) {
        this.messageId = messageId;
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getCorrelationId() {
        return correlationId;
    }

    @Override
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public byte[] getCorrelationIdBytes() {
        return correlationId.getBytes(UTF8);
    }

    @Override
    public void setCorrelationIdBytes(byte[] correlationId) {
        if (correlationId != null && correlationId.length > 0) {
            this.correlationId = new String(correlationId, UTF8);
        } else {
            this.correlationId = null;
        }
    }

    @Override
    public boolean isPersistent() {
        return this.persistent;
    }

    @Override
    public void setPersistent(boolean value) {
        this.persistent = value;
    }

    @Override
    public int getRedeliveryCounter() {
        return this.redeliveryCount;
    }

    @Override
    public void setRedeliveryCounter(int redeliveryCount) {
        this.redeliveryCount = redeliveryCount;
    }

    @Override
    public boolean isRedelivered() {
        return redeliveryCount > 0;
    }

    @Override
    public void setRedelivered(boolean redelivered) {
        if (redelivered) {
            if (!isRedelivered()) {
                setRedeliveryCounter(1);
            }
        } else {
            if (isRedelivered()) {
                setRedeliveryCounter(0);
            }
        }
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public byte getPriority() {
        return priority;
    }

    @Override
    public void setPriority(byte priority) {
        this.priority = priority;
    }

    @Override
    public long getExpiration() {
        return expiration;
    }

    @Override
    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }

    @Override
    public JmsDestination getDestination() {
        return this.destination;
    }

    @Override
    public void setDestination(JmsDestination destination) {
        this.destination = destination;
    }

    @Override
    public JmsDestination getReplyTo() {
        return this.replyTo;
    }

    @Override
    public void setReplyTo(JmsDestination replyTo) {
        this.replyTo = replyTo;
    }

    @Override
    public String getUserId() {
        return this.userId;
    }

    @Override
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String getGroupId() {
        return this.groupId;
    }

    @Override
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Override
    public int getGroupSequence() {
        return this.groupSequence;
    }

    @Override
    public void setGroupSequence(int groupSequence) {
        this.groupSequence = groupSequence;
    }

    private void lazyCreateProperties() {
        if (properties == null) {
            properties = new HashMap<String, Object>();
        }
    }
}
