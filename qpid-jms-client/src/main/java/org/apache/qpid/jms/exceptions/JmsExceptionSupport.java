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

import javax.jms.IllegalStateException;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidClientIDRuntimeException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.InvalidSelectorException;
import javax.jms.InvalidSelectorRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageNotWriteableRuntimeException;
import javax.jms.ResourceAllocationException;
import javax.jms.ResourceAllocationRuntimeException;
import javax.jms.TransactionInProgressException;
import javax.jms.TransactionInProgressRuntimeException;
import javax.jms.TransactionRolledBackException;
import javax.jms.TransactionRolledBackRuntimeException;

import org.apache.qpid.jms.provider.ProviderException;

/**
 * Exception support class.
 *
 * Factory class for creating JMSException instances based on String messages or by
 * wrapping other non-JMS exception.
 *
 * @since 1.0
 */
public final class JmsExceptionSupport {

    private JmsExceptionSupport() {}

    /**
     * Creates or passes through a JMSException to be thrown to the client.
     *
     * In the event that the exception passed to this method is already a
     * JMSException it is passed through unmodified, otherwise a new JMSException
     * is created with the given message and the cause is set to the given
     * cause Throwable instance.
     *
     * @param message
     *        The message value to set when a new JMSException is created.
     * @param cause
     *        The exception that caused this error state.
     *
     * @return a JMSException instance.
     */
    public static JMSException create(String message, Throwable cause) {
        if (cause instanceof JMSException) {
            return (JMSException) cause;
        }

        if (cause.getCause() instanceof JMSException) {
            return (JMSException) cause.getCause();
        } else if (cause instanceof ProviderException) {
            return ((ProviderException) cause).toJMSException();
        }

        if (message == null || message.isEmpty()) {
            message = cause.getMessage();
            if (message == null || message.isEmpty()) {
                message = cause.toString();
            }
        }

        JMSException exception = new JMSException(message);
        if (cause instanceof Exception) {
            exception.setLinkedException((Exception) cause);
        }
        exception.initCause(cause);
        return exception;
    }

    /**
     * Creates or passes through a JMSException to be thrown to the client.
     *
     * In the event that the exception passed to this method is already a
     * JMSException it is passed through unmodified, otherwise a new JMSException
     * is created using the error message taken from the given Throwable value
     * and the cause value is set to the given Throwable instance.
     *
     * @param cause
     *        The exception that caused this error state.
     *
     * @return a JMSException instance.
     */
    public static JMSException create(Throwable cause) {
        return create(null, cause);
    }

    /**
     * Creates or passes through a MessageEOFException to be thrown to the client.
     *
     * In the event that the exception passed to this method is already a
     * MessageEOFException it is passed through unmodified, otherwise a new
     * MessageEOFException is created using the error message taken from the
     * given Throwable value and the cause value is set to the given Throwable
     * instance.
     *
     * @param cause
     *        The exception that caused this error state.
     *
     * @return a MessageEOFException instance.
     */
    public static MessageEOFException createMessageEOFException(Throwable cause) {
        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        MessageEOFException exception = new MessageEOFException(message);
        if (cause instanceof Exception) {
            exception.setLinkedException((Exception) cause);
        }
        exception.initCause(cause);
        return exception;
    }

    /**
     * Creates or passes through a MessageFormatException to be thrown to the client.
     *
     * In the event that the exception passed to this method is already a
     * MessageFormatException it is passed through unmodified, otherwise a new
     * MessageFormatException is created using the error message taken from the
     * given Throwable value and the cause value is set to the given Throwable
     * instance.
     *
     * @param cause
     *        The exception that caused this error state.
     *
     * @return a MessageEOFException instance.
     */
    public static MessageFormatException createMessageFormatException(Throwable cause) {
        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        MessageFormatException exception = new MessageFormatException(message);
        if (cause instanceof Exception) {
            exception.setLinkedException((Exception) cause);
        }
        exception.initCause(cause);
        return exception;
    }

    /**
     * Creates the proper instance of a JMSRuntimeException based on the type
     * of JMSException that is passed.
     *
     * @param exception
     *      The JMSException instance to convert to a JMSRuntimeException
     *
     * @return a new {@link JMSRuntimeException} instance that reflects the original error.
     */
    public static JMSRuntimeException createRuntimeException(Exception exception) {
        JMSRuntimeException result = null;
        JMSException source = null;

        if (!(exception instanceof JMSException)) {
            throw new JMSRuntimeException(exception.getMessage(), null, exception);
        } else {
            source = (JMSException) exception;
        }

        if (source instanceof IllegalStateException) {
            result = new IllegalStateRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof InvalidClientIDException) {
            result = new InvalidClientIDRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof InvalidDestinationException) {
            result = new InvalidDestinationRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof InvalidSelectorException) {
            result = new InvalidSelectorRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof JMSSecurityException) {
            result = new JMSSecurityRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof MessageFormatException) {
            result = new MessageFormatRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof MessageNotWriteableException) {
            result = new MessageNotWriteableRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof ResourceAllocationException) {
            result = new ResourceAllocationRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof TransactionInProgressException) {
            result = new TransactionInProgressRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else if (source instanceof TransactionRolledBackException) {
            result = new TransactionRolledBackRuntimeException(source.getMessage(), source.getErrorCode(), source);
        } else {
            result = new JMSRuntimeException(source.getMessage(), source.getErrorCode(), source);
        }

        return result;
    }
}
