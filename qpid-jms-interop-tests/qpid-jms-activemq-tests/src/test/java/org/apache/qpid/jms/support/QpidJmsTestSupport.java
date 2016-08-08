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
package org.apache.qpid.jms.support;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.JMXSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all protocol test support classes.
 */
public class QpidJmsTestSupport {

    public static final String MESSAGE_NUMBER = "MessageNumber";

    public static final String KAHADB_DIRECTORY = "target/activemq-data";

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(QpidJmsTestSupport.class);
    protected BrokerService brokerService;
    protected final List<BrokerService> brokers = new ArrayList<BrokerService>();
    protected final Vector<Throwable> exceptions = new Vector<Throwable>();
    protected int numberOfMessages;
    protected Connection connection;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== setUp " + getTestName() + " ==========");
        exceptions.clear();
        startPrimaryBroker();
        this.numberOfMessages = 2000;
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("========== tearDown " + getTestName() + " ==========");
        Exception firstError = null;

        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("Error detected on connection close in tearDown: {}", e.getMessage());
                firstError = e;
            }
        }

        try {
            stopPrimaryBroker();
        } catch (Exception e) {
            LOG.warn("Error detected on close of broker in tearDown: {}", e.getMessage());
            firstError = e;
        }

        for (BrokerService broker : brokers) {
            try {
                stopBroker(broker);
            } catch (Exception ex) {
                LOG.warn("Error detected on close of broker in tearDown: {}", ex.getMessage());
                firstError = ex;
            }
        }

        if (firstError != null) {
            throw firstError;
        }
    }

    public String getDestinationName() {
        return name.getMethodName();
    }

    public URI getBrokerActiveMQClientConnectionURI() {
        try {
            return new URI("vm://localhost");
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    protected boolean isPersistent() {
        return false;
    }

    protected boolean isConcurrentStoreAndDispatchQueues() {
        return false;
    }

    protected boolean isConcurrentStoreAndDispatchTopics() {
        return false;
    }

    protected boolean isAdvisorySupport() {
        return false;
    }

    protected boolean isSchedulerSupport() {
        return false;
    }

    protected BrokerService createBroker(String name, boolean deleteAllMessages) throws Exception {
        return createBroker(name, deleteAllMessages, Collections.<String, Integer> emptyMap());
    }

    protected void configureBrokerPolicies(BrokerService broker) {

    }

    protected BrokerService createBroker(String name, boolean deleteAllMessages, Map<String, Integer> portMap) throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName(name);
        brokerService.setPersistent(isPersistent());
        brokerService.setSchedulerSupport(isSchedulerSupport());
        brokerService.setAdvisorySupport(isAdvisorySupport());
        brokerService.setDeleteAllMessagesOnStartup(deleteAllMessages);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setDataDirectory("target/" + name);
        brokerService.setKeepDurableSubsActive(false);

        if (isPersistent()) {
            KahaDBStore kaha = new KahaDBStore();
            kaha.setDirectory(new File(KAHADB_DIRECTORY + "/" + name));
            kaha.setConcurrentStoreAndDispatchQueues(isConcurrentStoreAndDispatchQueues());
            kaha.setConcurrentStoreAndDispatchTopics(isConcurrentStoreAndDispatchTopics());
            kaha.setCheckpointInterval(TimeUnit.MINUTES.toMillis(5));
            brokerService.setPersistenceAdapter(kaha);
        }

        configureBrokerPolicies(brokerService);

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();
        BrokerPlugin authenticationPlugin = configureAuthentication();
        if (authenticationPlugin != null) {
            plugins.add(authenticationPlugin);
        }

        BrokerPlugin authorizationPlugin = configureAuthorization();
        if (authorizationPlugin != null) {
            plugins.add(authorizationPlugin);
        }

        addAdditionalBrokerPlugins(plugins);

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }

        addAdditionalConnectors(brokerService, portMap);

        return brokerService;
    }

    protected void addAdditionalBrokerPlugins(List<BrokerPlugin> plugins) {
        // Subclasses can add their own plugins, we don't add any here.
    }

    protected void addAdditionalConnectors(BrokerService brokerService, Map<String, Integer> portMap) throws Exception {
        // Subclasses can add their own connectors, we don't add any here.
    }

    public void startPrimaryBroker() throws Exception {
        if (brokerService != null && brokerService.isStarted()) {
            throw new IllegalStateException("Broker is already created.");
        }

        brokerService = createBroker("localhost", true);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public void restartPrimaryBroker() throws Exception {
        stopBroker(brokerService);
        brokerService = restartBroker(brokerService);
    }

    public void stopPrimaryBroker() throws Exception {
        stopBroker(brokerService);
    }

    public void startNewBroker() throws Exception {
        String brokerName = "localhost" + (brokers.size() + 1);

        BrokerService brokerService = createBroker(brokerName, true);
        brokerService.setUseJmx(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        brokers.add(brokerService);
    }

    public BrokerService restartBroker(BrokerService brokerService) throws Exception {
        String name = brokerService.getBrokerName();
        Map<String, Integer> portMap = new HashMap<String, Integer>();
        for (TransportConnector connector : brokerService.getTransportConnectors()) {
            portMap.put(connector.getName(), connector.getPublishableConnectURI().getPort());
        }

        stopBroker(brokerService);
        BrokerService broker = createBroker(name, false, portMap);
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    public void stopBroker(BrokerService broker) throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    public List<URI> getBrokerURIs() throws Exception {
        ArrayList<URI> result = new ArrayList<URI>();
        result.add(brokerService.getTransportConnectorByName("amqp").getPublishableConnectURI());

        for (BrokerService broker : brokers) {
            result.add(broker.getTransportConnectorByName("amqp").getPublishableConnectURI());
        }

        return result;
    }

    public Connection createActiveMQConnection() throws Exception {
        return createActiveMQConnection(getBrokerActiveMQClientConnectionURI());
    }

    public Connection createActiveMQConnection(URI brokerURI) throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);
        return factory.createConnection();
    }

    public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer p = session.createProducer(destination);

        for (int i = 1; i <= count; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("TextMessage: " + i);
            message.setIntProperty(MESSAGE_NUMBER, i);
            p.send(message);
        }

        session.close();
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        return getProxyToBroker(brokerService);
    }

    protected BrokerViewMBean getProxyToBroker(BrokerService broker) throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = broker.getBrokerObjectName();
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected ConnectorViewMBean getProxyToConnectionView(String connectionType) throws Exception {
        return getProxyToConnectionView(connectionType);
    }

    protected ConnectorViewMBean getProxyToConnectionView(BrokerService broker, String connectionType) throws Exception {
        ObjectName connectorQuery = new ObjectName(
            broker.getBrokerObjectName() + ",connector=clientConnectors,connectorName="+connectionType+"_//*");

        Set<ObjectName> results = brokerService.getManagementContext().queryNames(connectorQuery, null);
        if (results == null || results.isEmpty() || results.size() > 1) {
            throw new Exception("Unable to find the exact Connector instance.");
        }

        ConnectorViewMBean proxy = (ConnectorViewMBean) brokerService.getManagementContext()
                .newProxyInstance(results.iterator().next(), ConnectorViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToQueue(brokerService, name);
    }

    protected QueueViewMBean getProxyToQueue(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=Queue,destinationName=" + name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToTemporaryQueue(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToTemporaryQueue(brokerService, name);
    }

    protected QueueViewMBean getProxyToTemporaryQueue(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        name = JMXSupport.encodeObjectNamePart(name);
        ObjectName queueViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=TempQueue,destinationName=" + name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToTopic(brokerService, name);
    }

    protected TopicViewMBean getProxyToTopic(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        ObjectName topicViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=Topic,destinationName=" + name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
                .newProxyInstance(topicViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }

    protected TopicViewMBean getProxyToTemporaryTopic(String name) throws MalformedObjectNameException, JMSException {
        return getProxyToTemporaryTopic(brokerService, name);
    }

    protected TopicViewMBean getProxyToTemporaryTopic(BrokerService broker, String name) throws MalformedObjectNameException, JMSException {
        name = JMXSupport.encodeObjectNamePart(name);
        ObjectName topicViewMBeanName = new ObjectName(
            broker.getBrokerObjectName() + ",destinationType=TempTopic,destinationName=" + name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
            .newProxyInstance(topicViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }

    protected void sendToAmqQueue(int count) throws Exception {
        Connection activemqConnection = createActiveMQConnection();
        Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue amqTestQueue = amqSession.createQueue(name.getMethodName());
        sendMessages(activemqConnection, amqTestQueue, count);
        activemqConnection.close();
    }

    protected void sendToAmqTopic(int count) throws Exception {
        Connection activemqConnection = createActiveMQConnection();
        Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic amqTestTopic = amqSession.createTopic(name.getMethodName());
        sendMessages(activemqConnection, amqTestTopic, count);
        activemqConnection.close();
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        users.add(new AuthenticationUser("user", "password", "users"));
        users.add(new AuthenticationUser("guest", "password", "guests"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);
        authenticationPlugin.setAnonymousAccessAllowed(true);

        return authenticationPlugin;
    }

    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins,anonymous");
        entry.setAdmin("admins,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users,anonymous");
        entry.setWrite("users,anonymous");
        entry.setAdmin("users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins,anonymous");
        entry.setAdmin("admins,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users,anonymous");
        entry.setWrite("users,anonymous");
        entry.setAdmin("users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins,anonymous");
        tempEntry.setWrite("admins,anonymous");
        tempEntry.setAdmin("admins,anonymous");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }

    protected boolean isForceAsyncSends() {
        return false;
    }

    protected boolean isForceSyncSends() {
        return false;
    }

    protected boolean isForceAsyncAcks() {
        return false;
    }

    protected String getTestName() {
        return getClass().getSimpleName() + "." + name.getMethodName();
    }
}
