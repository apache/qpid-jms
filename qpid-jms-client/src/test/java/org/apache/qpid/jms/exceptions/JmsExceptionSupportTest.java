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
package org.apache.qpid.jms.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import jakarta.jms.IllegalStateException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.MessageNotWriteableRuntimeException;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.ResourceAllocationRuntimeException;
import jakarta.jms.TransactionInProgressException;
import jakarta.jms.TransactionInProgressRuntimeException;
import jakarta.jms.TransactionRolledBackException;
import jakarta.jms.TransactionRolledBackRuntimeException;

import org.junit.jupiter.api.Test;

/**
 * Tests for various utility methods in the Exception support class.
 */
public class JmsExceptionSupportTest {

    private final String ERROR_MESSAGE = "ExpectedErrorMessage";
    private final String CAUSE_MESSAGE = "ExpectedCauseMessage";

    private final IOException NO_MESSAGE_CAUSE = new IOException();
    private final IOException EMPTY_MESSAGE_CAUSE = new IOException("");

    @Test
    public void testCreateAssignsLinkedException() {
        JMSException result = JmsExceptionSupport.create(ERROR_MESSAGE, new IOException(CAUSE_MESSAGE));
        assertNotNull(result.getLinkedException());
    }

    @Test
    public void testCreateDoesNotFillLinkedExceptionWhenGivenNonExceptionThrowable() {
        JMSException result = JmsExceptionSupport.create(ERROR_MESSAGE, new AssertionError(CAUSE_MESSAGE));
        assertNull(result.getLinkedException());
    }

    @Test
    public void testCreateFillsMessageFromMessageParam() {
        JMSException result = JmsExceptionSupport.create(ERROR_MESSAGE, new IOException(CAUSE_MESSAGE));
        assertEquals(ERROR_MESSAGE, result.getMessage());
    }

    @Test
    public void testCreateFillsMessageFromMCauseessageParamMessage() {
        JMSException result = JmsExceptionSupport.create(new IOException(CAUSE_MESSAGE));
        assertEquals(CAUSE_MESSAGE, result.getMessage());
    }

    @Test
    public void testCreateFillsMessageFromMCauseessageParamToString() {
        JMSException result = JmsExceptionSupport.create(NO_MESSAGE_CAUSE);
        assertEquals(NO_MESSAGE_CAUSE.toString(), result.getMessage());
    }

    @Test
    public void testCreateFillsMessageFromMCauseessageParamToStringWhenMessageIsEmpty() {
        JMSException result = JmsExceptionSupport.create(EMPTY_MESSAGE_CAUSE);
        assertEquals(EMPTY_MESSAGE_CAUSE.toString(), result.getMessage());
    }

    @Test
    public void testCreateFillsMessageFromCauseMessageParamWhenErrorMessageIsNull() {
        JMSException result = JmsExceptionSupport.create(null, new IOException(CAUSE_MESSAGE));
        assertEquals(CAUSE_MESSAGE, result.getMessage());
    }

    @Test
    public void testCreateFillsMessageFromCauseMessageParamWhenErrorMessageIsEmpty() {
        JMSException result = JmsExceptionSupport.create("", new IOException(CAUSE_MESSAGE));
        assertEquals(CAUSE_MESSAGE, result.getMessage());
    }

    @Test
    public void testCreateMessageFormatExceptionFillsMessageFromMCauseessageParamToString() {
        JMSException result = JmsExceptionSupport.createMessageFormatException(NO_MESSAGE_CAUSE);
        assertEquals(NO_MESSAGE_CAUSE.toString(), result.getMessage());
    }

    @Test
    public void testCreateMessageFormatExceptionFillsMessageFromMCauseessageParamToStringWhenMessageIsEmpty() {
        JMSException result = JmsExceptionSupport.createMessageFormatException(EMPTY_MESSAGE_CAUSE);
        assertEquals(EMPTY_MESSAGE_CAUSE.toString(), result.getMessage());
    }

    @Test
    public void testCreateMessageEOFExceptionFillsMessageFromMCauseessageParamToString() {
        JMSException result = JmsExceptionSupport.createMessageEOFException(NO_MESSAGE_CAUSE);
        assertEquals(NO_MESSAGE_CAUSE.toString(), result.getMessage());
    }

    @Test
    public void testCreateMessageEOFExceptionFillsMessageFromMCauseessageParamToStringWhenMessageIsEmpty() {
        JMSException result = JmsExceptionSupport.createMessageEOFException(EMPTY_MESSAGE_CAUSE);
        assertEquals(EMPTY_MESSAGE_CAUSE.toString(), result.getMessage());
    }

    @Test
    public void testCreateMessageFormatExceptionAssignsLinkedException() {
        JMSException result = JmsExceptionSupport.createMessageFormatException(new IOException(CAUSE_MESSAGE));
        assertNotNull(result.getLinkedException());
    }

    @Test
    public void testCreateMessageFormatExceptionDoesNotFillLinkedExceptionWhenGivenNonExceptionThrowable() {
        JMSException result = JmsExceptionSupport.createMessageFormatException(new AssertionError(CAUSE_MESSAGE));
        assertNull(result.getLinkedException());
    }

    @Test
    public void testCreateMessageEOFExceptionAssignsLinkedException() {
        JMSException result = JmsExceptionSupport.createMessageEOFException(new IOException(CAUSE_MESSAGE));
        assertNotNull(result.getLinkedException());
    }

    @Test
    public void testCreateMessageEOFExceptionDoesNotFillLinkedExceptionWhenGivenNonExceptionThrowable() {
        JMSException result = JmsExceptionSupport.createMessageEOFException(new AssertionError(CAUSE_MESSAGE));
        assertNull(result.getLinkedException());
    }

    @Test
    public void testConvertsJMSExceptionToJMSRuntimeException() {
        assertThrows(JMSRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new JMSException("error"));
        });
    }

    @Test
    public void testConvertsIllegalStateExceptionToIlleglStateRuntimeException() {
        assertThrows(IllegalStateRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new IllegalStateException("error"));
        });
    }

    @Test
    public void testConvertsInvalidClientIDExceptionToInvalidClientIDRuntimeException() {
        assertThrows(InvalidClientIDRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new InvalidClientIDException("error"));
        });
    }

    @Test
    public void testConvertsInvalidDestinationExceptionToInvalidDestinationRuntimeException() {
        assertThrows(InvalidDestinationRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new InvalidDestinationException("error"));
        });
    }

    @Test
    public void testConvertsInvalidSelectorExceptionToInvalidSelectorRuntimeException() {
        assertThrows(InvalidSelectorRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new InvalidSelectorException("error"));
        });
    }

    @Test
    public void testConvertsJMSSecurityExceptionToJMSSecurityRuntimeException() {
        assertThrows(JMSSecurityRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new JMSSecurityException("error"));
        });
    }

    @Test
    public void testConvertsMessageFormatExceptionToMessageFormatRuntimeException() {
        assertThrows(MessageFormatRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new MessageFormatException("error"));
        });
    }

    @Test
    public void testConvertsMessageNotWriteableExceptionToMessageNotWriteableRuntimeException() {
        assertThrows(MessageNotWriteableRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new MessageNotWriteableException("error"));
        });
    }

    @Test
    public void testConvertsResourceAllocationExceptionToResourceAllocationRuntimeException() {
        assertThrows(ResourceAllocationRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new ResourceAllocationException("error"));
        });
    }

    @Test
    public void testConvertsTransactionInProgressExceptionToTransactionInProgressRuntimeException() {
        assertThrows(TransactionInProgressRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new TransactionInProgressException("error"));
        });
    }

    @Test
    public void testConvertsTransactionRolledBackExceptionToTransactionRolledBackRuntimeException() {
        assertThrows(TransactionRolledBackRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new TransactionRolledBackException("error"));
        });
    }

    @Test
    public void testConvertsNonJMSExceptionToJMSRuntimeException() {
        assertThrows(JMSRuntimeException.class, () -> {
            throw JmsExceptionSupport.createRuntimeException(new IOException());
        });
    }
}
