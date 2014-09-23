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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.support.Wait;

/**
 * Test Connection methods contracts when connection has failed.
 */
public class JmsConnectionFailedTest extends JmsConnectionClosedTest {

    @Override
    protected Connection createConnection() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        connection = createAmqpConnection();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                latch.countDown();
            }
        });
        connection.start();
        stopPrimaryBroker();
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        final JmsConnection jmsConnection = (JmsConnection) connection;
        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return !jmsConnection.isConnected();
            }
        }));
        return connection;
    }
}
