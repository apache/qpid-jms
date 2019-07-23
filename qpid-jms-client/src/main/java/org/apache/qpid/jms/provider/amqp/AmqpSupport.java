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
package org.apache.qpid.jms.provider.amqp;

import java.util.Map;

import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRedirectedException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRemotelyClosedException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionResourceAllocationException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionResourceNotFoundException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionSecurityException;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidClientIDException;
import org.apache.qpid.jms.provider.exceptions.ProviderInvalidDestinationException;
import org.apache.qpid.jms.provider.exceptions.ProviderResourceAllocationException;
import org.apache.qpid.jms.provider.exceptions.ProviderSecurityException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionRolledBackException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.TransactionErrors;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ConnectionError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Endpoint;

public class AmqpSupport {

    // Symbols used for connection capabilities
    public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    public static final Symbol SHARED_SUBS = Symbol.valueOf("SHARED-SUBS");

    // Symbols used to announce connection error information
    public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
    public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

    // Symbols used to announce failover server list (in addition to redirect symbols below)
    public static final Symbol FAILOVER_SERVER_LIST = Symbol.valueOf("failover-server-list");

    // Symbols used to announce connection redirect ErrorCondition 'info'
    public static final Symbol PATH = Symbol.valueOf("path");
    public static final Symbol SCHEME = Symbol.valueOf("scheme");
    public static final Symbol PORT = Symbol.valueOf("port");
    public static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
    public static final Symbol OPEN_HOSTNAME = Symbol.valueOf("hostname");

    // Symbols used for connection properties
    public static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
    public static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");

    public static final Symbol PRODUCT = Symbol.valueOf("product");
    public static final Symbol VERSION = Symbol.valueOf("version");
    public static final Symbol PLATFORM = Symbol.valueOf("platform");

    // Symbols used for receivers.
    public static final Symbol COPY = Symbol.getSymbol("copy");
    public static final Symbol JMS_NO_LOCAL_SYMBOL = Symbol.valueOf("no-local");
    public static final Symbol JMS_SELECTOR_SYMBOL = Symbol.valueOf("jms-selector");
    public static final Symbol SHARED = Symbol.valueOf("shared");
    public static final Symbol GLOBAL = Symbol.valueOf("global");

    // Delivery states
    public static final Rejected REJECTED = new Rejected();
    public static final Modified MODIFIED_FAILED = new Modified();
    public static final Modified MODIFIED_FAILED_UNDELIVERABLE = new Modified();

    // Temporary Destination constants
    public static final Symbol DYNAMIC_NODE_LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");
    public static final String TEMP_QUEUE_CREATOR = "temp-queue-creator:";
    public static final String TEMP_TOPIC_CREATOR = "temp-topic-creator:";

    // Subscription Name Delimiter
    public static final String SUB_NAME_DELIMITER = "|";

    //----- Static initializer -----------------------------------------------//

    static {
        MODIFIED_FAILED.setDeliveryFailed(true);

        MODIFIED_FAILED_UNDELIVERABLE.setDeliveryFailed(true);
        MODIFIED_FAILED_UNDELIVERABLE.setUndeliverableHere(true);
    }

    //----- Utility Methods --------------------------------------------------//

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates the connection creation failed for some reason.
     *
     * @param provider
     * 		the AMQP provider instance that originates this exception
     * @param endpoint
     *      The target of the error.
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ProviderConnectionRemotelyClosedException convertToConnectionClosedException(AmqpProvider provider, Endpoint endpoint, ErrorCondition errorCondition) {
        ProviderConnectionRemotelyClosedException remoteError = null;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                remoteError = new ProviderConnectionSecurityException(message);
            } else if (error.equals(AmqpError.RESOURCE_LIMIT_EXCEEDED)) {
                remoteError = new ProviderConnectionResourceAllocationException(message);
            } else if (error.equals(ConnectionError.CONNECTION_FORCED)) {
                remoteError = new ProviderConnectionRemotelyClosedException(message);
            } else if (error.equals(AmqpError.NOT_FOUND)) {
                remoteError = new ProviderConnectionResourceNotFoundException(message);
            } else if (error.equals(ConnectionError.REDIRECT)) {
                remoteError = createRedirectException(provider, error, message, errorCondition);
            } else if (error.equals(AmqpError.INVALID_FIELD)) {
                Map<?, ?> info = errorCondition.getInfo();
                if (info != null && CONTAINER_ID.equals(info.get(INVALID_FIELD))) {
                    remoteError = new ProviderInvalidClientIDException(message);
                } else {
                    remoteError = new ProviderConnectionRemotelyClosedException(message);
                }
            } else {
                remoteError = new ProviderConnectionRemotelyClosedException(message);
            }
        } else if (remoteError == null) {
            remoteError = new ProviderConnectionRemotelyClosedException("Unknown error from remote peer");
        }

        return remoteError;
    }

    /**
     * Given an ErrorCondition instance create a new Exception that best matches
     * the error type that indicates a non-fatal error usually at the link level
     * such as link closed remotely or link create failed due to security access
     * issues.
     *
     * @param provider
     * 		the AMQP provider instance that originates this exception
     * @param endpoint
     *      The target of the error.
     * @param errorCondition
     *      The ErrorCondition returned from the remote peer.
     *
     * @return a new Exception instance that best matches the ErrorCondition value.
     */
    public static ProviderException convertToNonFatalException(AmqpProvider provider, Endpoint endpoint, ErrorCondition errorCondition) {
        ProviderException remoteError = null;

        if (errorCondition != null && errorCondition.getCondition() != null) {
            Symbol error = errorCondition.getCondition();
            String message = extractErrorMessage(errorCondition);

            if (error.equals(AmqpError.UNAUTHORIZED_ACCESS)) {
                remoteError = new ProviderSecurityException(message);
            } else if (error.equals(AmqpError.RESOURCE_LIMIT_EXCEEDED)) {
                remoteError = new ProviderResourceAllocationException(message);
            } else if (error.equals(AmqpError.NOT_FOUND)) {
                remoteError = new ProviderInvalidDestinationException(message);
            } else if (error.equals(TransactionErrors.TRANSACTION_ROLLBACK)) {
                remoteError = new ProviderTransactionRolledBackException(message);
            } else {
                remoteError = new ProviderException(message);
            }
        } else if (remoteError == null) {
            remoteError = new ProviderException("Unknown error from remote peer");
        }

        return remoteError;
    }

    /**
     * Attempt to read and return the embedded error message in the given ErrorCondition
     * object.  If no message can be extracted a generic message is returned.
     *
     * @param errorCondition
     *      The ErrorCondition to extract the error message from.
     *
     * @return an error message extracted from the given ErrorCondition.
     */
    public static String extractErrorMessage(ErrorCondition errorCondition) {
        String message = "Received error from remote peer without description";
        if (errorCondition != null) {
            if (errorCondition.getDescription() != null && !errorCondition.getDescription().isEmpty()) {
                message = errorCondition.getDescription();
            }

            Symbol condition = errorCondition.getCondition();
            if (condition != null) {
                message = message + " [condition = " + condition + "]";
            }
        }

        return message;
    }

    /**
     * When a redirect type exception is received this method is called to create the
     * appropriate redirect exception type containing the error details needed.
     *
     * @param provider
     * 		  the AMQP provider instance that originates this exception
     * @param error
     *        the Symbol that defines the redirection error type.
     * @param message
     *        the basic error message that should used or amended for the returned exception.
     * @param condition
     *        the ErrorCondition that describes the redirection.
     *
     * @return an Exception that captures the details of the redirection error.
     */
    public static ProviderConnectionRemotelyClosedException createRedirectException(AmqpProvider provider, Symbol error, String message, ErrorCondition condition) {
        ProviderConnectionRemotelyClosedException result = null;
        Map<?, ?> info = condition.getInfo();

        if (info == null) {
            result = new ProviderConnectionRemotelyClosedException(message + " : Redirection information not set.");
        } else {
            @SuppressWarnings("unchecked")
            AmqpRedirect redirect = new AmqpRedirect((Map<Symbol, Object>) info, provider);

            try {
                result = new ProviderConnectionRedirectedException(message, redirect.validate().toURI());
            } catch (Exception ex) {
                result = new ProviderConnectionRemotelyClosedException(message + " : " + ex.getMessage());
            }
        }

        return result;
    }
}