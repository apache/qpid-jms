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
package org.apache.qpid.jms.session;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.test.Wait;


/**
 * Tests the Session method contracts when the underlying connection is lost.
 */
public class JmsSessionFailedTest extends JmsSessionClosedTest {

    @Override
    protected void createTestResources() throws Exception {
        connection = createConnectionToMockProvider();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
            }
        });
        Queue destination = session.createQueue(_testName.getMethodName());

        sender = session.createProducer(destination);
        receiver = session.createConsumer(destination);
        connection.start();
        providerListener.onConnectionFailure(new ProviderException("Something went wrong"));

        final JmsConnection jmsConnection = connection;
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
                return !jmsConnection.isConnected();
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(2)));
    }
}
