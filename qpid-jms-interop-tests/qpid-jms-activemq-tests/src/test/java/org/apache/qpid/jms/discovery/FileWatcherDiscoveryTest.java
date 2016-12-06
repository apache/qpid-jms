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
package org.apache.qpid.jms.discovery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsConnectionListener;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the file watcher Discovery Provider finds a broker URI in
 * the file it is directed to watch.
 */
public class FileWatcherDiscoveryTest extends AmqpTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FileWatcherDiscoveryTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("./target"));

    private CountDownLatch connected;
    private CountDownLatch interrupted;
    private CountDownLatch restored;
    private JmsConnection jmsConnection;

    private File primaryBrokerList;
    private File secondaryBrokerList;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        connected = new CountDownLatch(1);
        interrupted = new CountDownLatch(1);
        restored = new CountDownLatch(1);

        primaryBrokerList = folder.newFile("primaryBrokerURIsFile.txt");
        secondaryBrokerList = folder.newFile("secondaryBrokerURIsFile.txt");

        LOG.info("Broker URIs going to file: {}", primaryBrokerList);

        writeOutBrokerURIsToFile(primaryBrokerList);
    }

    @Test(timeout = 60000)
    public void testConnectedToStoredBrokerURI() throws Exception {
        assertTrue(primaryBrokerList.exists());

        connection = createConnection();
        connection.start();

        assertTrue("connection never connected.", connected.await(30, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testReconnectWhenURIUpdates() throws Exception {
        assertTrue(primaryBrokerList.exists());

        connection = createConnection();
        connection.start();

        assertTrue("connection never connected.", connected.await(30, TimeUnit.SECONDS));

        stopPrimaryBroker();

        assertTrue("connection should be interrupted.", interrupted.await(30, TimeUnit.SECONDS));

        startPrimaryBroker();

        writeOutBrokerURIsToFile(primaryBrokerList);

        assertTrue("connection should have been reestablished.", restored.await(30, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testReconnectUsingTwoFiles() throws Exception {
        assertTrue(primaryBrokerList.exists());
        assertTrue(secondaryBrokerList.exists());

        connection = createConnection(new File[]{ primaryBrokerList, secondaryBrokerList });
        connection.start();

        assertTrue("connection never connected.", connected.await(30, TimeUnit.SECONDS));

        stopPrimaryBroker();

        assertTrue("connection should be interrupted.", interrupted.await(30, TimeUnit.SECONDS));

        startPrimaryBroker();

        writeOutBrokerURIsToFile(secondaryBrokerList);

        assertTrue("connection should have been reestablished.", restored.await(30, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testWithInitiallyNonExistingFile() throws Exception {
        assertTrue(primaryBrokerList.exists());

        final String FILENAME = "nonExistentFile.txt";

        File nonExistentFile = new File(folder.getRoot(), FILENAME);
        assertFalse(nonExistentFile.exists());

        connection = createConnection(new File[]{ primaryBrokerList, nonExistentFile });
        connection.start();

        assertTrue("connection never connected.", connected.await(30, TimeUnit.SECONDS));

        stopPrimaryBroker();

        assertTrue("connection should be interrupted.", interrupted.await(30, TimeUnit.SECONDS));

        startPrimaryBroker();

        folder.newFile(FILENAME);
        assertTrue(nonExistentFile.exists());

        writeOutBrokerURIsToFile(nonExistentFile);

        assertTrue("connection should have been reestablished.", restored.await(30, TimeUnit.SECONDS));
    }

    protected Connection createConnection() throws Exception {
        return createConnection(new File[]{ primaryBrokerList });
    }

    protected Connection createConnection(File[] filesToWatch) throws Exception {

        StringBuilder fileURIs = new StringBuilder();
        for (File file : filesToWatch) {
            if (fileURIs.length() == 0) {
                fileURIs.append(file.toURI());
                fileURIs.append("?updateInterval=1000");
            } else {
                fileURIs.append(",");
                fileURIs.append(file.toURI());
                fileURIs.append("?updateInterval=1000");
            }
        }

        JmsConnectionFactory factory = new JmsConnectionFactory(
            "discovery:(" + fileURIs.toString() + ")?discovery.maxReconnectDelay=500");
        connection = factory.createConnection();

        jmsConnection = (JmsConnection) connection;
        jmsConnection.addConnectionListener(new JmsConnectionListener() {

            @Override
            public void onConnectionEstablished(URI remoteURI) {
                LOG.info("Connection reports established.  Connected to -> {}", remoteURI);
                connected.countDown();
            }

            @Override
            public void onConnectionInterrupted(URI remoteURI) {
                LOG.info("Connection reports interrupted. Lost connection to -> {}", remoteURI);
                interrupted.countDown();
            }

            @Override
            public void onConnectionRestored(URI remoteURI) {
                LOG.info("Connection reports restored.  Connected to -> {}", remoteURI);
                restored.countDown();
            }

            @Override
            public void onConnectionFailure(Throwable error) {
            }

            @Override
            public void onInboundMessage(JmsInboundMessageDispatch envelope) {
            }

            @Override
            public void onSessionClosed(Session session, Throwable exception) {
            }

            @Override
            public void onConsumerClosed(MessageConsumer consumer, Throwable cause) {
            }

            @Override
            public void onProducerClosed(MessageProducer producer, Throwable cause) {
            }
        });

        return connection;
    }

    protected void writeOutBrokerURIsToFile(File targetFile) throws Exception {
        try (FileOutputStream out = new FileOutputStream(targetFile);) {
            for (URI brokerURI : getBrokerURIs()) {
                LOG.info("Broker URI being written: {}", brokerURI);
                out.write(brokerURI.toString().getBytes("UTF-8"));
            }
        }
    }
}
