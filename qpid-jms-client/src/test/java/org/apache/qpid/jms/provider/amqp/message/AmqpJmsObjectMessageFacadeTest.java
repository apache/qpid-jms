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
package org.apache.qpid.jms.provider.amqp.message;

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.getSymbol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Test;

/**
 * Tests for class AmqpJmsObjectMessageFacade
 */
public class AmqpJmsObjectMessageFacadeTest extends AmqpJmsMessageTypesTestCase {

    // ---------- Test initial state of newly created message -----------------//

    @Test
    public void testNewMessageToSendContainsMessageTypeAnnotation() throws Exception {
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(true);

        Message protonMessage = amqpObjectMessageFacade.getAmqpMessage();
        MessageAnnotations annotations = protonMessage.getMessageAnnotations();
        Map<Symbol, Object> annotationsMap = annotations.getValue();

        assertNotNull("MessageAnnotations section was not present", annotations);
        assertNotNull("MessageAnnotations section value was not present", annotationsMap);

        assertTrue("expected message type annotation to be present", annotationsMap.containsKey(AmqpMessageSupport.getSymbol(JMS_MSG_TYPE)));
        assertEquals("unexpected value for message type annotation value", JMS_OBJECT_MESSAGE, annotationsMap.get(getSymbol(JMS_MSG_TYPE)));
        assertEquals(JMS_OBJECT_MESSAGE, amqpObjectMessageFacade.getJmsMsgType());
    }

    @Test
    public void testNewMessageToSendReturnsNullObject() throws Exception {
        doNewMessageToSendReturnsNullObjectTestImpl(true);
    }

    @Test
    public void testNewAmqpTypedMessageToSendReturnsNullObject() throws Exception {
        doNewMessageToSendReturnsNullObjectTestImpl(false);
    }

    private void doNewMessageToSendReturnsNullObjectTestImpl(boolean javaSerialized) throws Exception {
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createNewObjectMessageFacade(javaSerialized);
        amqpObjectMessageFacade.clearBody();
        assertNull(amqpObjectMessageFacade.getObject());
    }

    // ---------- test for normal message operations -------------------------//

    // ---------- test handling of received messages -------------------------//

    @Test
    public void testGetObjectUsingReceivedMessageWithNoBodySectionReturnsNull() throws Exception {
        Message message = Message.Factory.create();
        AmqpJmsObjectMessageFacade amqpObjectMessageFacade = createReceivedObjectMessageFacade(createMockAmqpConsumer(), message);

        assertNull("Expected null object", amqpObjectMessageFacade.getObject());
    }
}
