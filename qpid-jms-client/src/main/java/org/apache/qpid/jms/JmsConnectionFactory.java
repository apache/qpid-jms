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
package org.apache.qpid.jms;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.qpid.jms.exceptions.JmsExceptionSupport;
import org.apache.qpid.jms.jndi.JNDIStorable;
import org.apache.qpid.jms.message.JmsMessageIDBuilder;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.policy.JmsDefaultMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsDefaultPresettlePolicy;
import org.apache.qpid.jms.policy.JmsDefaultRedeliveryPolicy;
import org.apache.qpid.jms.policy.JmsMessageIDPolicy;
import org.apache.qpid.jms.policy.JmsPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsPresettlePolicy;
import org.apache.qpid.jms.policy.JmsRedeliveryPolicy;
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
    private static final String DEFAULT_REMOTE_HOST = "localhost";
    private static final String DEFAULT_REMOTE_PORT = "5672";

    public static final String REMOTE_URI_PROP = "remoteURI";

    private static String DEFAULT_REMOTE_URI;

    private URI remoteURI;
    private String username;
    private String password;
    private String clientID;
    private boolean forceAsyncSend;
    private boolean forceSyncSend;
    private boolean forceAsyncAcks;
    private boolean localMessagePriority;
    private boolean localMessageExpiry = true;
    private boolean receiveLocalOnly;
    private boolean receiveNoWaitLocalOnly;
    private boolean populateJMSXUserID;
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

    private JmsPrefetchPolicy prefetchPolicy = new JmsDefaultPrefetchPolicy();
    private JmsRedeliveryPolicy redeliveryPolicy = new JmsDefaultRedeliveryPolicy();
    private JmsPresettlePolicy presettlePolicy = new JmsDefaultPresettlePolicy();
    private JmsMessageIDPolicy messageIDPolicy = new JmsDefaultMessageIDPolicy();

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

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createTopicConnection(getUsername(), getPassword());
    }

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

    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(getUsername(), getPassword());
    }

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

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createQueueConnection(getUsername(), getPassword());
    }

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

            // Copy the configured policies before applying URI options that
            // might make additional configuration changes.
            connection.setMessageIDPolicy(messageIDPolicy.copy());
            connection.setPrefetchPolicy(prefetchPolicy.copy());
            connection.setPresettlePolicy(presettlePolicy.copy());
            connection.setRedeliveryPolicy(redeliveryPolicy.copy());

            PropertyUtil.setProperties(connection, properties);
            connection.setExceptionListener(exceptionListener);
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
        if (remoteURI == null) {
            remoteURI = new URI(getDefaultRemoteAddress());
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
        return this.remoteURI != null ? this.remoteURI.toString() : getDefaultRemoteAddress();
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

        if (this.remoteURI.getRawUserInfo() != null) {
            throw new IllegalArgumentException("The supplied URI cannot contain a User-Info section");
        }

        try {
            if (this.remoteURI.getQuery() != null) {
                Map<String, String> map = PropertyUtil.parseQuery(this.remoteURI.getQuery());
                applyURIOptions(map);
                this.remoteURI = PropertyUtil.replaceQuery(this.remoteURI, map);
            } else if (URISupport.isCompositeURI(this.remoteURI)) {
                CompositeData data = URISupport.parseComposite(this.remoteURI);
                applyURIOptions(data.getParameters());
                this.remoteURI = data.toURI();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private void applyURIOptions(Map<String, String> options) throws IllegalArgumentException {
        Map<String, String> jmsOptionsMap = PropertyUtil.filterProperties(options, "jms.");
        Map<String, String> unused = PropertyUtil.setProperties(this, jmsOptionsMap);
        if (!unused.isEmpty()) {
            String msg = ""
                + " Not all jms options could be set on the ConnectionFactory."
                + " Check the options are spelled correctly."
                + " Unused parameters=[" + unused + "]."
                + " This connection factory cannot be started.";
            throw new IllegalArgumentException(msg);
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

    /**
     * Returns true if the client should always send messages using a synchronous
     * send operation regardless of persistence mode, or inside a transaction.
     *
     * @return true if sends should always be done synchronously.
     */
    public boolean isForceSyncSend() {
        return forceSyncSend;
    }

    /**
     * Configures whether or not the client will always send messages synchronously or not
     * regardless of other factors that might result in an asynchronous send.
     *
     * @param forceSyncSend
     *        if true sends are always done synchronously.
     */
    public void setForceSyncSend(boolean forceSyncSend) {
        this.forceSyncSend = forceSyncSend;
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
        if (prefetchPolicy == null) {
            prefetchPolicy = new JmsDefaultPrefetchPolicy();
        }

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
        if (redeliveryPolicy == null) {
            redeliveryPolicy = new JmsDefaultRedeliveryPolicy();
        }
        this.redeliveryPolicy = redeliveryPolicy;
    }

    /**
     * @return the presettlePolicy that is currently configured.
     */
    public JmsPresettlePolicy getPresettlePolicy() {
        return presettlePolicy;
    }

    /**
     * Sets the JmsPresettlePolicy that is applied to MessageProducers.
     *
     * @param presettlePolicy
     *      the presettlePolicy to use by connections created from this factory.
     */
    public void setPresettlePolicy(JmsPresettlePolicy presettlePolicy) {
        if (presettlePolicy == null) {
            presettlePolicy = new JmsDefaultPresettlePolicy();
        }
        this.presettlePolicy = presettlePolicy;
    }

    /**
     * @return the messageIDPolicy that is currently configured.
     */
    public JmsMessageIDPolicy getMessageIDPolicy() {
        return messageIDPolicy;
    }

    /**
     * Sets the JmsMessageIDPolicy that is use to configure the JmsMessageIDBuilder
     * that is assigned to any new MessageProducer created from Connection instances
     * that this factory has created.
     *
     * @param messageIDPolicy
     *      the messageIDPolicy to use by connections created from this factory.
     */
    public void setMessageIDPolicy(JmsMessageIDPolicy messageIDPolicy) {
        if (messageIDPolicy == null) {
            messageIDPolicy = new JmsDefaultMessageIDPolicy();
        }
        this.messageIDPolicy = messageIDPolicy;
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
     *
     * @param clientID
     *      The clientID to assign when creating a new connection.
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
     * @return true if consumer acknowledgments are sent asynchronously or not.
     */
    public boolean isForceAsyncAcks() {
        return forceAsyncAcks;
    }

    /**
     * Should the message acknowledgments from a consumer be sent synchronously or
     * asynchronously.  Sending the acknowledgments asynchronously can increase the
     * performance of a consumer but opens up the possibility of a missed message
     * acknowledge should the connection be unstable.
     *
     * @param forceAsyncAcks
     *        true to have the client send all message acknowledgments asynchronously.
     */
    public void setForceAsyncAcks(boolean forceAsyncAcks) {
        this.forceAsyncAcks = forceAsyncAcks;
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
     * @deprecated use the jms.messageIDPolicy.messageIDType URI setting instead.
     *
     * @param type
     *      The name of the Message type to use when sending a message.
     */
    @Deprecated
    public void setMessageIDType(String type) {
        if (messageIDPolicy instanceof JmsDefaultMessageIDPolicy) {
            ((JmsDefaultMessageIDPolicy) messageIDPolicy).setMessageIDType(type);
        }
    }

    @Deprecated
    public String getMessageIDType() {
        if (messageIDPolicy instanceof JmsDefaultMessageIDPolicy) {
            return ((JmsDefaultMessageIDPolicy) this.messageIDPolicy).getMessageIDType();
        }

        return null;
    }

    /**
     * @return the messageIDBuilder currently configured.
     *
     * @deprecated Create a custom JmsMessageIDPolicy to control the JmsMessageIDBuilder
     */
    @Deprecated
    public JmsMessageIDBuilder getMessageIDBuilder() {
        if (messageIDPolicy instanceof JmsDefaultMessageIDPolicy) {
            return ((JmsDefaultMessageIDPolicy) this.messageIDPolicy).getMessageIDBuilder();
        }

        return null;
    }

    /**
     * Allows the owner of this factory to configure a custom Message ID Builder
     * instance that will be used to create the Message ID values set in outgoing
     * Messages sent from MessageProducer instances.
     *
     * @param messageIDBuilder
     *      The custom JmsMessageIDBuilder to use to create outgoing Message IDs.
     *
     * @deprecated Create a custom JmsMessageIDPolicy to control the JmsMessageIDBuilder
     */
    @Deprecated
    public void setMessageIDBuilder(JmsMessageIDBuilder messageIDBuilder) {
        if (messageIDPolicy instanceof JmsDefaultMessageIDPolicy) {
            ((JmsDefaultMessageIDPolicy) this.messageIDPolicy).setMessageIDBuilder(messageIDBuilder);
        }
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

    public boolean isPopulateJMSXUserID() {
        return populateJMSXUserID;
    }

    /**
     * Controls whether message sent from the Connection will have the JMSXUserID message
     * property populated with the authenticated user ID of the Connection.  When false all
     * messages sent from the Connection will not carry any value in the JMSXUserID property
     * regardless of it being manually set on the Message to prevent a client spoofing the
     * JMSXUserID value.
     *
     * @param populateJMSXUserID
     *      true if message sent from this connection should have the JMSXUserID value populated.
     */
    public void setPopulateJMSXUserID(boolean populateJMSXUserID) {
        this.populateJMSXUserID = populateJMSXUserID;
    }

    //----- Static Methods ---------------------------------------------------//

    /**
     * @return the default remote address to connect to in the event that none was set.
     */
    public static String getDefaultRemoteAddress() {

        if (DEFAULT_REMOTE_URI != null) {
            return DEFAULT_REMOTE_URI;
        }

        synchronized (JmsConnectionFactory.class) {

            if (DEFAULT_REMOTE_URI != null) {
                return DEFAULT_REMOTE_URI;
            }

            String host = null;
            String port = null;
            String remoteAddress = null;

            try {
                host = AccessController.doPrivileged(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        String result = System.getProperty("org.apache.qpid.jms.REMOTE_HOST", DEFAULT_REMOTE_HOST);
                        result = (result == null || result.isEmpty()) ? DEFAULT_REMOTE_HOST : result;
                        return result;
                    }
                });
                port = AccessController.doPrivileged(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        String result = System.getProperty("org.apache.qpid.jms.REMOTE_PORT", DEFAULT_REMOTE_PORT);
                        result = (result == null || result.isEmpty()) ? DEFAULT_REMOTE_PORT : result;
                        return result;
                    }
                });
            } catch (Throwable e) {
                LOG.debug("Failed to look up System properties for remote host and port", e);
            }

            host = (host == null || host.isEmpty()) ? DEFAULT_REMOTE_HOST : host;
            port = (port == null || port.isEmpty()) ? DEFAULT_REMOTE_PORT : port;

            final String defaultURL = "amqp://" + host + ":" + port;

            try {
                remoteAddress = AccessController.doPrivileged(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        String result = System.getProperty("org.apache.qpid.jms.REMOTE_URI", defaultURL);
                        result = (result == null || result.isEmpty()) ? defaultURL : result;
                        return result;
                    }
                });
            } catch (Throwable e) {
                LOG.debug("Failed to look up System property for remote URI", e);
            }

            DEFAULT_REMOTE_URI = (remoteAddress == null || remoteAddress.isEmpty()) ? defaultURL : remoteAddress;
        }

        return DEFAULT_REMOTE_URI;
    }
}
