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
package org.apache.qpid.jms.session;

import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.Test;

/**
 * Tests behaviour after a QueueSession is closed.
 */
public class JmsQueueSessionClosedTest extends AmqpTestSupport {

    private QueueSession session;
    private QueueSender sender;
    private QueueReceiver receiver;

    protected void createAndCloseSession() throws Exception {
        JmsConnectionFactory factory = new JmsConnectionFactory(getBrokerAmqpConnectionURI());
        connection = factory.createQueueConnection();

        session = ((QueueConnection) connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue(name.getMethodName());

        sender = session.createSender(destination);
        receiver = session.createReceiver(destination);

        // Close the session explicitly, without closing the above.
        session.close();
    }

    @Test(timeout=30000)
    public void testSessionCloseAgain() throws Exception {
        createAndCloseSession();
        // Close it again
        session.close();
    }

    @Test(timeout=30000)
    public void testReceiverCloseAgain() throws Exception {
        createAndCloseSession();
        // Close it again (closing the session should have closed it already).
        receiver.close();
    }

    @Test(timeout=30000)
    public void testSenderCloseAgain() throws Exception {
        createAndCloseSession();
        // Close it again (closing the session should have closed it already).
        sender.close();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testReceiverGetQueueFails() throws Exception {
        createAndCloseSession();
        receiver.getQueue();
    }

    @Test(timeout=30000, expected=javax.jms.IllegalStateException.class)
    public void testSenderGetQueueFails() throws Exception {
        createAndCloseSession();
        sender.getQueue();
    }
}
