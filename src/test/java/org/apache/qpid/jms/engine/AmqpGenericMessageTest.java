/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.apache.qpid.jms.impl.ClientProperties;
import org.junit.Before;
import org.junit.Test;

public class AmqpGenericMessageTest extends QpidJmsTestCase
{
    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
    }

    @Test
    public void testNewMessageToSendContainsMessageTypeAnnotation() throws Exception
    {
        AmqpGenericMessage amqpMessage = new AmqpGenericMessage();
        assertTrue("expected message type annotation to be present", amqpMessage.messageAnnotationExists(ClientProperties.X_OPT_JMS_MSG_TYPE));
        assertEquals("unexpected value for message type annotation value", ClientProperties.GENERIC_MESSAGE_TYPE, amqpMessage.getMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE));
    }
}
