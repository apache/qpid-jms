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
package org.apache.qpid.jms.producer;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.test.Wait;

/**
 * Tests the MessageProducer method contract when it's connection has failed.
 */
public class JmsMessageProducerFailedTest extends JmsMessageProducerClosedTest {

    @Override
    protected MessageProducer createProducer() throws Exception {
        connection = createConnectionToMockProvider();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        message = session.createMessage();
        destination = session.createQueue("test");
        MessageProducer producer = session.createProducer(destination);
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
            }
        });
        connection.start();
        providerListener.onConnectionFailure(new ProviderException("Something went wrong"));

        final JmsConnection jmsConnection = connection;
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return !jmsConnection.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(2)));

        return producer;
    }
}
