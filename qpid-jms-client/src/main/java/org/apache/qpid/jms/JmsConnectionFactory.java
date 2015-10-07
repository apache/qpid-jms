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
package org.apache.qpid.jms;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.jndi.JNDIStorable;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.util.IdGenerator;
import org.apache.qpid.jms.util.PropertyUtil;
import org.apache.qpid.jms.util.URISupport;
import org.apache.qpid.jms.util.URISupport.CompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS ConnectionFactory Implementation.
 */
public class JmsConnectionFactory extends JNDIStorable implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JmsConnectionFactory.class);
    private static final String CLIENT_ID_PROP = "clientID";

    public static final String REMOTE_URI_PROP = "remoteURI";

    private URI remoteURI;
    private String username;
    private String password;
    private String clientID;
    private boolean forceAsyncSend;
    private boolean alwaysSyncSend;
    private boolean sendAcksAsync;
    private boolean localMessagePriority;
    private boolean localMessageExpiry = true;
    private boolean receiveLocalOnly;
    private boolean receiveNoWaitLocalOnly;
    private String queuePrefix = null;
    private String topicPrefix = null;
    private boolean validatePropertyNames = true;
    private long sendTimeout = JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private long connectTimeout = JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    private IdGenerator clientIdGenerator;
    private String clientIDPrefix;
    private IdGenerator connectionIdGenerator;
    private String connectionIDPrefix;
    private ExceptionListener exceptionListener;

    private JmsPrefetchPolicy prefetchPolicy = new JmsPrefetchPolicy();
    private JmsRedeliveryPolicy redeliveryPolicy = new JmsRedeliveryPolicy();
    private JmsMessageIDBuilder messageIDBuilder = JmsMessageIDBuilder.BUILTIN.DEFAULT.createBuilder();

    public JmsConnectionFactory() {
    }

    public JmsConnectionFactory(String username, String password) {
        setUsername(username);
        setPassword(password);
    }

    public JmsConnectionFactory(String remoteURI) {
        this(createURI(remoteURI));
    }

    public JmsConnectionFactory(URI remoteURI) {
        setRemoteURI(remoteURI.toString());
    }

    public JmsConnectionFactory(String userName, String password, URI remoteURI) {
        setUsername(userName);
        setPassword(password);
        setRemoteURI(remoteURI.toString());
    }

    public JmsConnectionFactory(String userName, String password, String remoteURI) {
        setUsername(userName);
        setPassword(password);
        setRemoteURI(remoteURI);
    }

    @Override
    protected Map<String, String> buildFromProperties(Map<String, String> props) {
        // Apply the remoteURI in a consistent order before
        // any other properties, since as it may contain
        // some options within it.
        String remoteURI = props.remove(REMOTE_URI_PROP);
        if (remoteURI != null) {
            setRemoteURI(remoteURI);
        }

        return PropertyUtil.setProperties(this, props);
    }

    @Override
    protected void populateProperties(Map<String, String> props) {
        try {
            Map<String, String> result = PropertyUtil.getProperties(this);
            props.putAll(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return a TopicConnection
     * @throws JMSException
     * @see javax.jms.TopicConnectionFactory#createTopicConnection()
     */
    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createTopicConnection(getUsername(), getPassword());
    }

    /**
     * @param username
     * @param password
     * @return a TopicConnection
     * @throws JMSException
     * @see javax.jms.TopicConnectionFactory#createTopicConnection(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public TopicConnection createTopicConnection(String username, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(remoteURI);
            JmsTopicConnection result = new JmsTopicConnection(connectionId, provider, getClientIdGenerator());
            return configureConnection(result, username, password);
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    /**
     * @return a Connection
     * @throws JMSException
     * @see javax.jms.ConnectionFactory#createConnection()
     */
    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(getUsername(), getPassword());
    }

    /**
     * @param username
     * @param password
     * @return Connection
     * @throws JMSException
     * @see javax.jms.ConnectionFactory#createConnection(java.lang.String, java.lang.String)
     */
    @Override
    public Connection createConnection(String username, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(remoteURI);
            JmsConnection result = new JmsConnection(connectionId, provider, getClientIdGenerator());
            return configureConnection(result, username, password);
        } catch (Exception e) {
            throw JmsExceptionSupport.create("Failed to create connection to: " + getRemoteURI(), e);
        }
    }

    /**
     * @return a QueueConnection
     * @throws JMSException
     * @see javax.jms.QueueConnectionFactory#createQueueConnection()
     */
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createQueueConnection(getUsername(), getPassword());
    }

    /**
     * @param username
     * @param password
     * @return a QueueConnection
     * @throws JMSException
     * @see javax.jms.QueueConnectionFactory#createQueueConnection(java.lang.String,
     *      java.lang.String)
     */
    @Override
    public QueueConnection createQueueConnection(String username, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(remoteURI);
            JmsQueueConnection result = new JmsQueueConnection(connectionId, provider, getClientIdGenerator());
            return configureConnection(result, username, password);
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    protected <T extends JmsConnection> T configureConnection(T connection, String username, String password) throws JMSException {
        try {
            Map<String, String> properties = PropertyUtil.getProperties(this);
            // We must ensure that we apply the clientID last, since setting it on
            // the Connection object provokes establishing the underlying connection.
            boolean setClientID = false;
            if (properties.containsKey(CLIENT_ID_PROP)) {
                setClientID = true;
                properties.remove(CLIENT_ID_PROP);
            }

            PropertyUtil.setProperties(connection, properties);
            connection.setExceptionListener(exceptionListener);
            connection.setMessageIDBuilder(messageIDBuilder);
            connection.setUsername(username);
            connection.setPassword(password);
            connection.setConfiguredURI(remoteURI);
            if (setClientID) {
                connection.setClientID(clientID);
            }

            return connection;
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    protected Provider createProvider(URI remoteURI) throws Exception {
        if(remoteURI == null) {
            throw new IllegalStateException("No remoteURI has been provided");
        }

        Provider result = null;

        try {
            result = ProviderFactory.create(remoteURI);
            result.connect();
        } catch (Exception ex) {
            LOG.error("Failed to create JMS Provider instance for: {}", remoteURI.getScheme());
            LOG.trace("Error: ", ex);
            throw ex;
        }

        return result;
    }

    protected static URI createURI(String name) {
        if (name != null && name.trim().isEmpty() == false) {
            try {
                return new URI(name);
            } catch (URISyntaxException e) {
                throw (IllegalArgumentException) new IllegalArgumentException("Invalid remote URI: " + name).initCause(e);
            }
        }
        return null;
    }

    protected synchronized IdGenerator getConnectionIdGenerator() {
        if (connectionIdGenerator == null) {
            if (connectionIDPrefix != null) {
                connectionIdGenerator = new IdGenerator(connectionIDPrefix);
            } else {
                connectionIdGenerator = new IdGenerator();
            }
        }
        return connectionIdGenerator;
    }

    protected synchronized void setConnectionIdGenerator(IdGenerator connectionIdGenerator) {
        this.connectionIdGenerator = connectionIdGenerator;
    }

    //////////////////////////////////////////////////////////////////////////
    // Property getters and setters
    //////////////////////////////////////////////////////////////////////////

    /**
     * @return the remoteURI
     */
    public String getRemoteURI() {
        return this.remoteURI != null ? this.remoteURI.toString() : "";
    }

    /**
     * @param remoteURI
     *        the remoteURI to set
     */
    public void setRemoteURI(String remoteURI) {
        if (remoteURI == null) {
            throw new IllegalArgumentException("remoteURI cannot be null");
        }
        this.remoteURI = createURI(remoteURI);

        try {
            if (this.remoteURI.getQuery() != null) {
                Map<String, String> map = PropertyUtil.parseQuery(this.remoteURI.getQuery());
                Map<String, String> jmsOptionsMap = PropertyUtil.filterProperties(map, "jms.");

                Map<String, String> unused = PropertyUtil.setProperties(this, jmsOptionsMap);
                if (!unused.isEmpty()) {
                    String msg = ""
                        + " Not all jms options could be set on the ConnectionFactory."
                        + " Check the options are spelled correctly."
                        + " Unused parameters=[" + unused + "]."
                        + " This connection factory cannot be started.";
                    throw new IllegalArgumentException(msg);
                } else {
                    this.remoteURI = PropertyUtil.replaceQuery(this.remoteURI, map);
                }
            } else if (URISupport.isCompositeURI(this.remoteURI)) {
                CompositeData data = URISupport.parseComposite(this.remoteURI);
                Map<String, String> jmsOptionsMap = PropertyUtil.filterProperties(data.getParameters(), "jms.");
                Map<String, String> unused = PropertyUtil.setProperties(this, jmsOptionsMap);
                if (!unused.isEmpty()) {
                    String msg = ""
                        + " Not all jms options could be set on the ConnectionFactory."
                        + " Check the options are spelled correctly."
                        + " Unused parameters=[" + unused + "]."
                        + " This connection factory cannot be started.";
                    throw new IllegalArgumentException(msg);
                } else {
                    this.remoteURI = data.toURI();
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * @return the user name used for connection authentication.
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * @param username
     *        the user name to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password set for connection authentication.
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * @param password
     *        the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isForceAsyncSend() {
        return forceAsyncSend;
    }

    public void setForceAsyncSend(boolean forceAsyncSend) {
        this.forceAsyncSend = forceAsyncSend;
    }

    /**
     * @return the localMessagePriority configuration option.
     */
    public boolean isLocalMessagePriority() {
        return this.localMessagePriority;
    }

    /**
     * Enables client-side message priority support in MessageConsumer instances.
     * This results in all prefetched messages being dispatched in priority order.
     *
     * @param localMessagePriority the messagePrioritySupported to set
     */
    public void setLocalMessagePriority(boolean localMessagePriority) {
        this.localMessagePriority = localMessagePriority;
    }

    /**
     * Returns the prefix applied to Queues that are created by the client.
     *
     * @return the currently configured Queue prefix.
     */
    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    /**
     * Returns the prefix applied to Topics that are created by the client.
     *
     * @return the currently configured Topic prefix.
     */
    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public boolean isValidatePropertyNames() {
        return validatePropertyNames;
    }

    public void setValidatePropertyNames(boolean validatePropertyNames) {
        this.validatePropertyNames = validatePropertyNames;
    }

    /**
     * Gets the currently set close timeout.
     *
     * @return the currently set close timeout.
     */
    public long getCloseTimeout() {
        return closeTimeout;
    }

    /**
     * Sets the close timeout used to control how long a Connection close will wait for
     * clean shutdown of the connection before giving up.  A negative value means wait
     * forever.
     *
     * Care should be taken in that a very short close timeout can cause the client to
     * not cleanly shutdown the connection and it's resources.
     *
     * @param closeTimeout
     *        time in milliseconds to wait for a clean connection close.
     */
    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    /**
     * Returns the currently configured wire level connect timeout.
     *
     * @return the currently configured wire level connect timeout.
     */
    public long getConnectTimeout() {
        return this.connectTimeout;
    }

    /**
     * Sets the timeout value used to control how long a client will wait for a successful
     * connection to the remote peer to be established before considering the attempt to
     * have failed.  This value does not control socket level connection timeout but rather
     * connection handshake at the wire level, to control the socket level timeouts use the
     * standard socket options configuration values.
     *
     * @param connectTimeout
     *        the time in milliseconds to wait for the protocol connection handshake to complete.
     */
    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public long getSendTimeout() {
        return sendTimeout;
    }

    public void setSendTimeout(long sendTimeout) {
        this.sendTimeout = sendTimeout;
    }

    public long getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(long requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public JmsPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(JmsPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    /**
     * Returns the JmsRedeliveryPolicy that is applied when a new connection is created.
     *
     * @return the redeliveryPolicy that is currently configured for this factory.
     */
    public JmsRedeliveryPolicy getRedeliveryPolicy() {
        return redeliveryPolicy;
    }

    /**
     * Sets the JmsRedeliveryPolicy that is applied when a new connection is created.
     *
     * @param redeliveryPolicy
     *        The new redeliveryPolicy to set
     */
    public void setRedeliveryPolicy(JmsRedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicy = redeliveryPolicy;
    }

    /**
     * @return the currently configured client ID prefix for auto-generated client IDs.
     */
    public synchronized String getClientIDPrefix() {
        return clientIDPrefix;
    }

    /**
     * Sets the prefix used by auto-generated JMS Client ID values which are used if the JMS
     * client does not explicitly specify one.
     *
     * @param clientIDPrefix
     *        the value to use as a prefix on auto-generated client IDs.
     */
    public synchronized void setClientIDPrefix(String clientIDPrefix) {
        this.clientIDPrefix = clientIDPrefix;
    }

    protected synchronized IdGenerator getClientIdGenerator() {
        if (clientIdGenerator == null) {
            if (clientIDPrefix != null) {
                clientIdGenerator = new IdGenerator(clientIDPrefix);
            } else {
                clientIdGenerator = new IdGenerator();
            }
        }
        return clientIdGenerator;
    }

    protected synchronized void setClientIdGenerator(IdGenerator clientIdGenerator) {
        this.clientIdGenerator = clientIdGenerator;
    }

    public String getClientID() {
        return clientID;
    }

    /**
     * Sets the JMS clientID to use for connections created by this factory.
     *
     * NOTE: A clientID can only be used by one Connection at a time, so setting it here
     * will restrict the ConnectionFactory to creating a single open Connection at a time.
     * It is possible to set the clientID on the Connection itself immediately after
     * creation if no value has been set via the factory that created it, which will
     * allow the factory to create multiple open connections at a time.
     */
    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    /**
     * Sets the prefix used by connection id generator.
     *
     * @param connectionIDPrefix
     *        The string prefix used on all connection Id's created by this factory.
     */
    public synchronized void setConnectionIDPrefix(String connectionIDPrefix) {
        this.connectionIDPrefix = connectionIDPrefix;
    }

    /**
     * Gets the currently configured JMS ExceptionListener that will be set on all
     * new Connection objects created from this factory.
     *
     * NOTE: the listener object is not saved when serializing the factory.
     *
     * @return the currently configured JMS ExceptionListener.
     */
    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    /**
     * Sets the JMS ExceptionListener that will be set on all new Connection objects
     * created from this factory.
     *
     * @param exceptionListener
     *        the JMS ExceptionListener to apply to new Connection's or null to clear.
     */
    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    /**
     * Returns true if the client should always send messages using a synchronous
     * send operation regardless of persistence mode, or inside a transaction.
     *
     * @return true if sends should always be done synchronously.
     */
    public boolean isAlwaysSyncSend() {
        return alwaysSyncSend;
    }

    /**
     * Configures whether or not the client will always send messages synchronously or not
     * regardless of other factors that might result in an asynchronous send.
     *
     * @param alwaysSyncSend
     *        if true sends are always done synchronously.
     */
    public void setAlwaysSyncSend(boolean alwaysSyncSend) {
        this.alwaysSyncSend = alwaysSyncSend;
    }

    /**
     * @return true if consumer acknowledgments are sent asynchronously or not.
     */
    public boolean isSendAcksAsync() {
        return sendAcksAsync;
    }

    /**
     * Should the message acknowledgments from a consumer be sent synchronously or
     * asynchronously.  Sending the acknowledgments asynchronously can increase the
     * performance of a consumer but opens up the possibility of a missed message
     * acknowledge should the connection be unstable.
     *
     * @param sendAcksAsync
     *        true to have the client send all message acknowledgments asynchronously.
     */
    public void setSendAcksAsync(boolean sendAcksAsync) {
        this.sendAcksAsync = sendAcksAsync;
    }

    /**
     * @return true if MessageConsumer instance will check for expired messages locally before dispatch.
     */
    public boolean isLocalMessageExpiry() {
        return localMessageExpiry;
    }

    /**
     * Controls whether message expiration checking is done locally (in addition to any broker
     * side checks) in each MessageConsumer prior to dispatching a message.  Disabling this check
     * can lead to consumption of expired messages.
     *
     * @param localMessageExpiry
     *        controls whether expiration checking is done prior to dispatch.
     */
    public void setLocalMessageExpiry(boolean localMessageExpiry) {
        this.localMessageExpiry = localMessageExpiry;
    }

    /**
     * Sets the type of the Message IDs used to populate the outgoing Messages
     *
     * @param type
     *      The name of the Message type to use when sending a message.
     */
    public void setMessageIDType(String type) {
        this.messageIDBuilder = JmsMessageIDBuilder.BUILTIN.create(type);
    }

    public String getMessageIDType() {
        return this.messageIDBuilder.toString();
    }

    /**
     * @return the messageIDBuilder currently configured.
     */
    public JmsMessageIDBuilder getMessageIDBuilder() {
        return messageIDBuilder;
    }

    /**
     * Allows the owner of this factory to configure a custom Message ID Builder
     * instance that will be used to create the Message ID values set in outgoing
     * Messages sent from MessageProducer instances.
     *
     * @param messageIDBuilder
     *      The custom JmsMessageIDBuilder to use to create outgoing Message IDs.
     */
    public void setMessageIDBuilder(JmsMessageIDBuilder messageIDBuilder) {
        if (messageIDBuilder == null) {
            messageIDBuilder = JmsMessageIDBuilder.BUILTIN.DEFAULT.createBuilder();
        }
        this.messageIDBuilder = messageIDBuilder;
    }

    public boolean isReceiveLocalOnly() {
        return receiveLocalOnly;
    }

    /**
     * Controls whether the client only checks its local message buffer when using
     * receive calls with a timeout, or will instead drain remaining credit from the
     * remote peer to ensure there are really no messages available if the
     * timeout expires before a message arrives in the consumers local buffer.
     *
     * @param receiveLocalOnly
     *        true if receive calls with a timeout should only check the local message buffer.
     */
    public void setReceiveLocalOnly(boolean receiveLocalOnly) {
        this.receiveLocalOnly = receiveLocalOnly;
    }

    public boolean isReceiveNoWaitLocalOnly() {
        return receiveNoWaitLocalOnly;
    }

    /**
     * Controls whether the client only checks its local message buffer when using
     * receiveNoWait calls, or will instead drain remaining credit from the
     * remote peer synchronously to ensure there are really no messages available
     * that have yet to arrive in the consumers local buffer.
     *
     * @param receiveNoWaitLocalOnly
     *        true if receiveNoWait calls should only check the local message buffer.
     */
    public void setReceiveNoWaitLocalOnly(boolean receiveNoWaitLocalOnly) {
        this.receiveNoWaitLocalOnly = receiveNoWaitLocalOnly;
    }
}
