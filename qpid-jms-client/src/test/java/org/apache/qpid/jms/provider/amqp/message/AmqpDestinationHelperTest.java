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

import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.QUEUE_ATTRIBUTES_STRING;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME;
import static org.apache.qpid.jms.provider.amqp.message.AmqpDestinationHelper.TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.mockito.Mockito;

public class AmqpDestinationHelperTest {

    private final AmqpDestinationHelper helper = AmqpDestinationHelper.INSTANCE;

    @Test
    public void testGetJmsDestinationWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);
        Mockito.when(message.getAnnotation(
            TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);

        assertNull(helper.getJmsDestination(message, null));
    }

    @Test
    public void testGetJmsReplyToWithNullAddressAndNullConsumerDestReturnsNull() throws Exception {
        AmqpJmsMessageFacade message = Mockito.mock(AmqpJmsMessageFacade.class);
        Mockito.when(message.getToAddress()).thenReturn(null);
        Mockito.when(message.getAnnotation(
            REPLY_TO_TYPE_MSG_ANNOTATION_SYMBOL_NAME)).thenReturn(QUEUE_ATTRIBUTES_STRING);

        assertNull(helper.getJmsDestination(message, null));
    }
}
