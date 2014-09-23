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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

    private URI brokerURI;
    private URI localURI;
    private String username;
    private String password;
    private boolean forceAsyncSend;
    private boolean alwaysSyncSend;
    private boolean sendAcksAsync;
    private boolean omitHost;
    private boolean messagePrioritySupported = true;
    private String queuePrefix = "queue://";
    private String topicPrefix = "topic://";
    private String tempQueuePrefix = "temp-queue://";
    private String tempTopicPrefix = "temp-topic://";
    private long sendTimeout = JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private long connectTimeout = JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    private boolean watchRemoteDestinations = true;
    private IdGenerator clientIdGenerator;
    private String clientIDPrefix;
    private IdGenerator connectionIdGenerator;
    private String connectionIDPrefix;
    private ExceptionListener exceptionListener;

    private JmsPrefetchPolicy prefetchPolicy = new JmsPrefetchPolicy();

    public JmsConnectionFactory() {
    }

    public JmsConnectionFactory(String username, String password) {
        setUsername(username);
        setPassword(password);
    }

    public JmsConnectionFactory(String brokerURI) {
        this(createURI(brokerURI));
    }

    public JmsConnectionFactory(URI brokerURI) {
        setBrokerURI(brokerURI.toString());
    }

    public JmsConnectionFactory(String userName, String password, URI brokerURI) {
        setUsername(userName);
        setPassword(password);
        setBrokerURI(brokerURI.toString());
    }

    public JmsConnectionFactory(String userName, String password, String brokerURI) {
        setUsername(userName);
        setPassword(password);
        setBrokerURI(brokerURI);
    }

    /**
     * Set properties
     *
     * @param props
     */
    public void setProperties(Properties props) {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        setProperties(map);
    }

    @Override
    public void setProperties(Map<String, String> map) {
        buildFromProperties(map);
    }

    /**
     * @param map
     */
    @Override
    protected void buildFromProperties(Map<String, String> map) {
        PropertyUtil.setProperties(this, map);
    }

    /**
     * @param map
     */
    @Override
    protected void populateProperties(Map<String, String> map) {
        try {
            Map<String, String> result = PropertyUtil.getProperties(this);
            map.putAll(result);
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
     * @param userName
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
            Provider provider = createProvider(brokerURI);
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
     * @param userName
     * @param password
     * @return Connection
     * @throws JMSException
     * @see javax.jms.ConnectionFactory#createConnection(java.lang.String, java.lang.String)
     */
    @Override
    public Connection createConnection(String username, String password) throws JMSException {
        try {
            String connectionId = getConnectionIdGenerator().generateId();
            Provider provider = createProvider(brokerURI);
            JmsConnection result = new JmsConnection(connectionId, provider, getClientIdGenerator());
            return configureConnection(result, username, password);
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
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
     * @param userName
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
            Provider provider = createProvider(brokerURI);
            JmsQueueConnection result = new JmsQueueConnection(connectionId, provider, getClientIdGenerator());
            return configureConnection(result, username, password);
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    protected <T extends JmsConnection> T configureConnection(T connection, String username, String password) throws JMSException {
        try {
            PropertyUtil.setProperties(connection, PropertyUtil.getProperties(this));
            connection.setExceptionListener(exceptionListener);
            connection.setUsername(username);
            connection.setPassword(password);
            connection.setBrokerURI(brokerURI);
            return connection;
        } catch (Exception e) {
            throw JmsExceptionSupport.create(e);
        }
    }

    protected Provider createProvider(URI brokerURI) throws Exception {
        Provider result = null;

        try {
            result = ProviderFactory.createAsync(brokerURI);
        } catch (Exception ex) {
            LOG.error("Failed to create JMS Provider instance for: {}", brokerURI.getScheme());
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
                throw (IllegalArgumentException) new IllegalArgumentException("Invalid broker URI: " + name).initCause(e);
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

    protected void setConnectionIdGenerator(IdGenerator connectionIdGenerator) {
        this.connectionIdGenerator = connectionIdGenerator;
    }

    //////////////////////////////////////////////////////////////////////////
    // Property getters and setters
    //////////////////////////////////////////////////////////////////////////

    /**
     * @return the brokerURI
     */
    public String getBrokerURI() {
        return this.brokerURI != null ? this.brokerURI.toString() : "";
    }

    /**
     * @param brokerURI
     *        the brokerURI to set
     */
    public void setBrokerURI(String brokerURI) {
        if (brokerURI == null) {
            throw new IllegalArgumentException("brokerURI cannot be null");
        }
        this.brokerURI = createURI(brokerURI);

        try {
            if (this.brokerURI.getQuery() != null) {
                Map<String, String> map = PropertyUtil.parseQuery(this.brokerURI.getQuery());
                Map<String, String> jmsOptionsMap = PropertyUtil.filterProperties(map, "jms.");

                if (!PropertyUtil.setProperties(this, jmsOptionsMap)) {
                    String msg = ""
                        + " Not all jms options could be set on the ConnectionFactory."
                        + " Check the options are spelled correctly."
                        + " Given parameters=[" + jmsOptionsMap + "]."
                        + " This connection factory cannot be started.";
                    throw new IllegalArgumentException(msg);
                } else {
                    this.brokerURI = PropertyUtil.replaceQuery(this.brokerURI, map);
                }
            } else if (URISupport.isCompositeURI(this.brokerURI)) {
                CompositeData data = URISupport.parseComposite(this.brokerURI);
                Map<String, String> jmsOptionsMap = PropertyUtil.filterProperties(data.getParameters(), "jms.");
                if (!PropertyUtil.setProperties(this, jmsOptionsMap)) {
                    String msg = ""
                        + " Not all jms options could be set on the ConnectionFactory."
                        + " Check the options are spelled correctly."
                        + " Given parameters=[" + jmsOptionsMap + "]."
                        + " This connection factory cannot be started.";
                    throw new IllegalArgumentException(msg);
                } else {
                    this.brokerURI = data.toURI();
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     * @return the localURI
     */
    public String getLocalURI() {
        return this.localURI != null ? this.localURI.toString() : "";
    }

    /**
     * @param localURI
     *        the localURI to set
     */
    public void setLocalURI(String localURI) {
        this.localURI = createURI(localURI);
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * @param username
     *        the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
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

    public boolean isOmitHost() {
        return omitHost;
    }

    public void setOmitHost(boolean omitHost) {
        this.omitHost = omitHost;
    }

    /**
     * @return the messagePrioritySupported configuration option.
     */
    public boolean isMessagePrioritySupported() {
        return this.messagePrioritySupported;
    }

    /**
     * Enables message priority support in MessageConsumer instances.  This results
     * in all prefetched messages being dispatched in priority order.
     *
     * @param messagePrioritySupported the messagePrioritySupported to set
     */
    public void setMessagePrioritySupported(boolean messagePrioritySupported) {
        this.messagePrioritySupported = messagePrioritySupported;
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
     * Returns the prefix applied to Temporary Queues that are created by the client.
     *
     * @return the currently configured Temporary Queue prefix.
     */
    public String getTempQueuePrefix() {
        return tempQueuePrefix;
    }

    public void setTempQueuePrefix(String tempQueuePrefix) {
        this.tempQueuePrefix = tempQueuePrefix;
    }

    /**
     * Returns the prefix applied to Temporary Topics that are created by the client.
     *
     * @return the currently configured Temporary Topic prefix.
     */
    public String getTempTopicPrefix() {
        return tempTopicPrefix;
    }

    public void setTempTopicPrefix(String tempTopicPrefix) {
        this.tempTopicPrefix = tempTopicPrefix;
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

    public String getClientIDPrefix() {
        return clientIDPrefix;
    }

    /**
     * Sets the prefix used by auto-generated JMS Client ID values which are used if the JMS
     * client does not explicitly specify on.
     *
     * @param clientIDPrefix
     */
    public void setClientIDPrefix(String clientIDPrefix) {
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

    protected void setClientIdGenerator(IdGenerator clientIdGenerator) {
        this.clientIdGenerator = clientIdGenerator;
    }

    /**
     * Sets the prefix used by connection id generator.
     *
     * @param connectionIDPrefix
     *        The string prefix used on all connection Id's created by this factory.
     */
    public void setConnectionIDPrefix(String connectionIDPrefix) {
        this.connectionIDPrefix = connectionIDPrefix;
    }

    /**
     * Gets the currently configured JMS ExceptionListener that will be set on all
     * new Connection objects created from this factory.
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
     *        the JMS ExceptionListenenr to apply to new Connection's or null to clear.
     */
    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    /**
     * Indicates if the Connection's created from this factory will watch for updates
     * from the remote peer informing of temporary destination creation and destruction.
     *
     * @return true if destination monitoring is enabled.
     */
    public boolean isWatchRemoteDestinations() {
        return watchRemoteDestinations;
    }

    /**
     * Enable or disable monitoring of remote temporary destination life-cycles.
     *
     * @param watchRemoteDestinations
     *        true if connection instances should monitor remote destination life-cycles.
     */
    public void setWatchRemoteDestinations(boolean watchRemoteDestinations) {
        this.watchRemoteDestinations = watchRemoteDestinations;
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
}
