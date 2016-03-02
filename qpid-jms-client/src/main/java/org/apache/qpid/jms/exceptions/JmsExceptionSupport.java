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

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

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
}
