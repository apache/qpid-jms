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
package org.apache.qpid.jms.provider.failover;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.TransactionRolledBackException;

import org.apache.qpid.jms.JmsOperationTimedOutException;
import org.apache.qpid.jms.JmsSendTimedOutException;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.meta.JmsTransactionInfo;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderRedirectedException;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.jms.util.QpidJMSThreadFactory;
import org.apache.qpid.jms.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Provider Facade that provides services for detection dropped Provider connections
 * and attempting to reconnect to a different remote peer.  Upon establishment of a new
 * connection the FailoverProvider will initiate state recovery of the active JMS
 * framework resources.
 */
public class FailoverProvider extends DefaultProviderListener implements Provider {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverProvider.class);

    public static final int UNLIMITED = -1;

    private static final int UNDEFINED = -1;
    private static final int DISABLED = 0;
    private static final int MINIMUM_TIMEOUT = 1000;

    public static final int DEFAULT_MAX_RECONNECT_ATTEMPTS = UNLIMITED;
    public static final int DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS = UNDEFINED;
    public static final long DEFAULT_INITIAL_RECONNECT_DELAY = 0;
    public static final long DEFAULT_RECONNECT_DELAY = 10;
    public static final long DEFAULT_MAX_RECONNECT_DELAY = TimeUnit.SECONDS.toMillis(30);
    public static final boolean DEFAULT_USE_RECONNECT_BACKOFF = true;
    public static final double DEFAULT_RECONNECT_BACKOFF_MULTIPLIER = 2.0d;
    public static final int DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS = 10;

    private ProviderListener listener;
    private Provider provider;
    private final FailoverUriPool uris;
    private ScheduledFuture<?> requestTimeoutTask;

    private final ScheduledThreadPoolExecutor serializer;
    private final ScheduledThreadPoolExecutor connectionHub;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicBoolean closingConnection = new AtomicBoolean(false);
    private final AtomicLong requestId = new AtomicLong();
    private final Map<Long, FailoverRequest> requests = new LinkedHashMap<Long, FailoverRequest>();
    private final DefaultProviderListener closedListener = new DefaultProviderListener();
    private final AtomicReference<JmsMessageFactory> messageFactory = new AtomicReference<JmsMessageFactory>();

    // Current state of connection / reconnection
    private boolean firstAttempt = true;
    private boolean firstConnection = true;
    private long reconnectAttempts;
    private long nextReconnectDelay = -1;
    private IOException failureCause;
    private URI connectedURI;
    private volatile JmsConnectionInfo connectionInfo;

    // Timeout values configured via JmsConnectionInfo
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private long sendTimeout = JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;

    // Configuration values.
    private long initialReconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
    private long reconnectDelay = DEFAULT_RECONNECT_DELAY;
    private long maxReconnectDelay = DEFAULT_MAX_RECONNECT_DELAY;
    private boolean useReconnectBackOff = DEFAULT_USE_RECONNECT_BACKOFF;
    private double reconnectBackOffMultiplier = DEFAULT_RECONNECT_BACKOFF_MULTIPLIER;
    private int maxReconnectAttempts = DEFAULT_MAX_RECONNECT_ATTEMPTS;
    private int startupMaxReconnectAttempts = DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS;
    private int warnAfterReconnectAttempts = DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS;

    private FailoverServerListAction amqpOpenServerListAction = FailoverServerListAction.REPLACE;

    public FailoverProvider(Map<String, String> nestedOptions) {
        this(null, nestedOptions);
    }

    public FailoverProvider(List<URI> uris) {
        this(uris, null);
    }

    public FailoverProvider(List<URI> uris, Map<String, String> nestedOptions) {
        this.uris = new FailoverUriPool(uris, nestedOptions);

        serializer = new ScheduledThreadPoolExecutor(1, new QpidJMSThreadFactory("FailoverProvider: serialization thread", true));
        serializer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        serializer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        // All Connection attempts happen in this schedulers thread.  Once a connection
        // is established it will hand the open connection back to the serializer thread
        // for state recovery.
        connectionHub = new ScheduledThreadPoolExecutor(1, new QpidJMSThreadFactory("FailoverProvider: connect thread", true));
        connectionHub.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        connectionHub.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    @Override
    public void connect(JmsConnectionInfo connectionInfo) throws IOException {
        checkClosed();
        this.connectionInfo = connectionInfo;
        LOG.debug("Initiating initial connection attempt task");
        triggerReconnectionAttempt();
    }

    @Override
    public void start() throws IOException, IllegalStateException {
        checkClosed();

        if (listener == null) {
            throw new IllegalStateException("No ProviderListener registered.");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderFuture request = new ProviderFuture();
            serializer.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        IOException error = failureCause != null ? failureCause : new IOException("Connection closed");
                        List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                        for (FailoverRequest request : pending) {
                            request.onFailure(error);
                        }

                        if (requestTimeoutTask != null) {
                            requestTimeoutTask.cancel(false);
                        }

                        if (provider != null) {
                            provider.close();
                        }
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing connection");
                    } finally {
                        ThreadPoolUtils.shutdownGraceful(connectionHub);
                        if (serializer != null) {
                            serializer.shutdown();
                        }
                        request.onSuccess();
                    }
                }
            });

            try {
                if (this.closeTimeout < 0) {
                    request.sync();
                } else {
                    request.sync(Math.max(MINIMUM_TIMEOUT, closeTimeout), TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            }
        }
    }

    @Override
    public void create(final JmsResource resource, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        FailoverRequest pending = null;
        if (resource instanceof JmsConnectionInfo) {
            pending = new CreateConnectionRequest(request) {
                @Override
                public void doTask() throws Exception {
                    JmsConnectionInfo connectionInfo = (JmsConnectionInfo) resource;

                    // Collect the timeouts we will handle in this provider.
                    closeTimeout = connectionInfo.getCloseTimeout();
                    sendTimeout = connectionInfo.getSendTimeout();
                    requestTimeout = connectionInfo.getRequestTimeout();

                    provider.create(resource, this);
                }

                @Override
                public String toString() {
                    return "create -> " + resource;
                }
            };
        } else {
            pending = new FailoverRequest(request, requestTimeout) {
                @Override
                public void doTask() throws Exception {
                    provider.create(resource, this);
                }

                @Override
                public boolean succeedsWhenOffline() {
                    if (resource instanceof JmsTransactionInfo) {
                        // Tag as in-doubt and let recovery on reconnect sort it out.
                        JmsTransactionInfo transactionInfo = (JmsTransactionInfo) resource;
                        transactionInfo.setInDoubt(true);

                        return true;
                    } else {
                        return false;
                    }
                }

                @Override
                public String toString() {
                    return "create -> " + resource;
                }
            };
        }

        serializer.execute(pending);
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.start(resource, this);
            }

            @Override
            public String toString() {
                return "start -> " + resource;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void stop(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.stop(resource, this);
            }

            @Override
            public String toString() {
                return "stop -> " + resource;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void destroy(final JmsResource resourceId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws IOException, JMSException, UnsupportedOperationException {
                if (resourceId instanceof JmsConnectionInfo) {
                   closingConnection.set(true);
                }
                provider.destroy(resourceId, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, resource won't get recreated on reconnect.
                return true;
            }

            @Override
            public String toString() {
                return "destroy -> " + resourceId;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, sendTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.send(envelope, this);
            }

            @Override
            public String toString() {
                return "send -> " + envelope;
            }

            @Override
            public JMSException createTimedOutException() {
                return new JmsSendTimedOutException("Timed out waiting on " + this, envelope.getMessage());
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final ACK_TYPE ackType, AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.acknowledge(sessionId, ackType, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acks would be stale.
                return true;
            }

            @Override
            public String toString() {
                return "session acknowledge -> " + sessionId;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.acknowledge(envelope, ackType, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acks would be stale.
                return true;
            }

            @Override
            public String toString() {
                return "message acknowledge -> " + envelope + " ackType: " + ackType;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void commit(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.commit(transactionInfo, nextTransactionInfo, this);
            }

            @Override
            public boolean failureWhenOffline() {
                return true;
            }

            @Override
            public String toString() {
                return "TX commit -> " + transactionInfo.getId();
            }

            @Override
            protected Exception createOfflineFailureException(IOException error) {
                Exception ex = new TransactionRolledBackException("Commit failed, connection offline: " + error.getMessage());
                ex.initCause(error);
                return ex;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void rollback(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.rollback(transactionInfo, nextTransactionInfo, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                return true;
            }

            @Override
            public String toString() {
                return "TX rollback -> " + transactionInfo.getId();
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult request) throws IOException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.recover(sessionId, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                return true;
            }

            @Override
            public String toString() {
                return "recover -> " + sessionId;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void unsubscribe(final String subscription, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask() throws Exception {
                provider.unsubscribe(subscription, this);
            }

            @Override
            public String toString() {
                return "unsubscribe -> " + subscription;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void pull(final JmsConsumerId consumerId, final long timeout, final AsyncResult request) throws IOException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.pull(consumerId, timeout, this);
            }

            @Override
            public String toString() {
                return "message pull -> " + consumerId;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        return messageFactory.get();
    }

    //--------------- Connection Error and Recovery methods ------------------//

    /**
     * This method is always called from within the FailoverProvider's serialization thread.
     *
     * When a failure is encountered either from an outgoing request or from an error fired
     * from the underlying Provider instance this method is called to determine if a reconnect
     * is allowed and if so a new reconnect cycle is triggered on the connection thread.
     *
     * @param cause
     *        the error that triggered the failure of the provider.
     */
    private void handleProviderFailure(final IOException cause) {
        if (provider != null) {
            LOG.debug("handling Provider failure: {}", cause.getMessage());
            LOG.trace("stack", cause);

            provider.setProviderListener(closedListener);
            URI failedURI = this.provider.getRemoteURI();
            try {
                provider.close();
            } catch (Throwable error) {
                LOG.trace("Caught exception while closing failed provider: {}", error.getMessage());
            }
            provider = null;

            if (reconnectAllowed(cause)) {

                if (cause instanceof ProviderRedirectedException) {
                    ProviderRedirectedException redirect = (ProviderRedirectedException) cause;
                    try {
                        uris.addFirst(redirect.getRedirectionURI());
                    } catch (Exception error) {
                        LOG.warn("Could not construct redirection URI from remote provided information");
                    }
                }

                ProviderListener listener = this.listener;
                if (listener != null) {
                    listener.onConnectionInterrupted(failedURI);
                }

                // Start watching for request timeouts while we are offline, unless we already are.
                if (requestTimeoutTask == null) {
                    long sweeperInterval = getRequestSweeperInterval();
                    if (sweeperInterval > 0) {
                        LOG.trace("Request timeout monitoring enabled: interval = {}ms", sweeperInterval);
                        requestTimeoutTask = serializer.scheduleWithFixedDelay(
                            new FailoverRequestSweeper(), sweeperInterval, sweeperInterval, TimeUnit.MILLISECONDS);
                    }
                }

                triggerReconnectionAttempt();
            } else {
                ProviderListener listener = this.listener;
                if (listener != null) {
                    listener.onConnectionFailure(cause);
                }
            }
        }
    }

    /**
     * Called from the reconnection thread.  This method enqueues a new task that
     * will attempt to recover connection state, once successful, normal operations
     * will resume.  If an error occurs while attempting to recover the JMS framework
     * state then a reconnect cycle is again triggered on the connection thread.
     *
     * @param provider
     *        The newly connect Provider instance that will become active.
     */
    private void initializeNewConnection(final Provider provider) {
        this.serializer.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    FailoverProvider.this.provider = provider;
                    provider.setProviderListener(FailoverProvider.this);

                    if (!firstConnection) {
                        LOG.debug("Signalling connection recovery: {}", provider);

                        // Stage 1: Allow listener to recover its resources
                        listener.onConnectionRecovery(provider);

                        // Stage 2: Connection state recovered, get newly configured message factory.
                        FailoverProvider.this.messageFactory.set(provider.getMessageFactory());

                        // Stage 3: Restart consumers, send pull commands, etc.
                        listener.onConnectionRecovered(provider);

                        // Stage 4: Let the client know that connection has restored.
                        listener.onConnectionRestored(provider.getRemoteURI());
                    }

                    // Last step: Send pending actions.
                    List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                    for (FailoverRequest request : pending) {
                        request.run();
                    }

                    nextReconnectDelay = reconnectDelay;
                    reconnectAttempts = 0;
                    connectedURI = provider.getRemoteURI();
                    uris.connected();

                    // Cancel timeout processing since we are connected again.  We waited until
                    // now for the case where we are continually getting bounced from otherwise
                    // live servers, we want the timeout to remain scheduled in that case so that
                    // it doesn't keep getting rescheduled and never actually time anything out.
                    if (requestTimeoutTask != null) {
                        requestTimeoutTask.cancel(false);
                        requestTimeoutTask = null;
                    }

                } catch (Throwable error) {
                    handleProviderFailure(IOExceptionSupport.create(error));
                }
            }
        });
    }

    /**
     * Called when the Provider was either first created or when a connection failure has
     * been reported.  A reconnection attempt is immediately executed on the connection
     * thread.  If a new Provider is able to be created and connected then a recovery task
     * is scheduled on the main serializer thread.  If the connect attempt fails another
     * attempt is scheduled based on the configured delay settings until a max attempts
     * limit is hit, if one is set.
     *
     * Since the initialize is put on the serializer thread this thread stops and does
     * not queue another connect task.  This allows for the reconnect delay to be reset
     * and a failure to initialize a new connection restarts the connect process from the
     * point of view that connection was lost and an immediate attempt cycle should start.
     */
    private void triggerReconnectionAttempt() {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        connectionHub.execute(new Runnable() {
            boolean delayed = false;

            @Override
            public void run() {
                if (provider != null || closingConnection.get() || closed.get() || failed.get()) {
                    return;
                }

                int reconnectLimit = reconnectAttemptLimit();
                if (reconnectLimit != UNLIMITED && reconnectAttempts >= reconnectLimit) {
                    return;
                }

                boolean first = firstAttempt;
                firstAttempt = false;

                if (!delayed && !first && initialReconnectDelay > 0 && reconnectAttempts == 0) {
                    delayed = true;
                    LOG.trace("Delayed initial reconnect attempt will be in {} milliseconds", initialReconnectDelay);
                    connectionHub.schedule(this, initialReconnectDelay, TimeUnit.MILLISECONDS);
                    return;
                }

                reconnectAttempts++;
                Throwable failure = null;
                URI target = uris.getNext();
                if (target != null) {
                    Provider provider = null;
                    try {
                        LOG.debug("Connection attempt:[{}] to: {} in-progress", reconnectAttempts, target);
                        provider = ProviderFactory.create(target);
                        provider.connect(connectionInfo);
                        initializeNewConnection(provider);
                        return;
                    } catch (Throwable e) {
                        LOG.info("Connection attempt:[{}] to: {} failed", reconnectAttempts, target);
                        failure = e;
                        try {
                            if (provider != null) {
                                provider.close();
                            }
                        } catch (Throwable ex) {}
                    }
                } else {
                    LOG.debug("No target URI available to connect to");
                }

                if (reconnectLimit != UNLIMITED && reconnectAttempts >= reconnectLimit) {
                    LOG.error("Failed to connect after: " + reconnectAttempts + " attempt(s)");
                    failed.set(true);
                    if (failure == null) {
                        failureCause = new IOException("Failed to connect after: " + reconnectAttempts + " attempt(s)");
                    } else {
                        failureCause = IOExceptionSupport.create(failure);
                    }
                    if (listener != null) {
                        listener.onConnectionFailure(failureCause);
                    };

                    return;
                }

                int warnInterval = getWarnAfterReconnectAttempts();
                if (warnInterval > 0 && (reconnectAttempts % warnInterval) == 0) {
                    LOG.warn("Failed to connect after: {} attempt(s) continuing to retry.", reconnectAttempts);
                }

                long delay = nextReconnectDelay();
                LOG.trace("Next reconnect attempt will be in {} milliseconds", delay);
                connectionHub.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        });
    }

    private boolean reconnectAllowed(IOException cause) {
        // If a connection attempts fail due to Security errors than
        // we abort reconnection as there is a configuration issue and
        // we want to avoid a spinning reconnect cycle that can never
        // complete.
        if (cause.getCause() instanceof JMSSecurityException) {
            return false;
        }

        return reconnectAttemptLimit() != 0;
    }

    private int reconnectAttemptLimit() {
        int maxReconnectValue = this.maxReconnectAttempts;
        if (firstConnection && this.startupMaxReconnectAttempts != UNDEFINED) {
            // If this is the first connection and a specific startup retry limit
            // is configured then use it, otherwise use the main reconnect limit
            maxReconnectValue = this.startupMaxReconnectAttempts;
        }
        return maxReconnectValue;
    }

    private long nextReconnectDelay() {

        if (nextReconnectDelay == -1) {
            nextReconnectDelay = reconnectDelay;
        }

        if (isUseReconnectBackOff() && reconnectAttempts > 1) {
            // Exponential increment of reconnect delay.
            nextReconnectDelay *= getReconnectBackOffMultiplier();
            if (nextReconnectDelay > maxReconnectDelay) {
                nextReconnectDelay = maxReconnectDelay;
            }
        }

        return nextReconnectDelay;
    }

    protected void checkClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("The Provider is already closed");
        }
    }

    //--------------- DefaultProviderListener overrides ----------------------//

    @Override
    public void onInboundMessage(final JmsInboundMessageDispatch envelope) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        listener.onInboundMessage(envelope);
    }

    @Override
    public void onCompletedMessageSend(final JmsOutboundMessageDispatch envelope) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        listener.onCompletedMessageSend(envelope);
    }

    @Override
    public void onFailedMessageSend(final JmsOutboundMessageDispatch envelope, Throwable cause) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        listener.onFailedMessageSend(envelope, cause);
    }

    @Override
    public void onConnectionFailure(final IOException ex) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closingConnection.get() && !closed.get() && !failed.get()) {
                    LOG.debug("Failover: the provider reports failure: {}", ex.getMessage());
                    handleProviderFailure(ex);
                }
            }
        });
    }

    @Override
    public void onProviderException(final Exception ex) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closingConnection.get() && !closed.get() && !failed.get()) {
                    LOG.debug("Failover: the provider reports an async error: {}", ex.getMessage());
                    listener.onProviderException(ex);
                }
            }
        });
    }

    @Override
    public void onRemoteDiscovery(final List<URI> discovered) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        if (discovered == null || discovered.isEmpty()) {
            return;
        }

        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closingConnection.get() && !closed.get() && !failed.get()) {

                    List<URI> newRemotes = new ArrayList<URI>(discovered);
                    switch (amqpOpenServerListAction) {
                        case ADD:
                            try {
                                uris.addAll(discovered);
                            } catch (Throwable err) {
                                LOG.warn("Error while attempting to add discovered URIs: {}", discovered);
                            }
                            break;
                        case REPLACE:
                            // The current server is assumed not to be in the list of updated remote
                            // as it is meant for the failover nodes. The pool will de-dup if it is.
                            newRemotes.add(0, connectedURI);
                            try {
                                uris.replaceAll(newRemotes);
                            } catch (Throwable err) {
                                LOG.warn("Error while attempting to add discovered URIs: {}", discovered);
                            }
                            break;
                        case IGNORE:
                            // Do Nothing
                            break;
                        default:
                            // Shouldnt get here, but do nothing if we do.
                            break;
                    }

                    // Inform any listener that we've made a new discovery.
                    if (listener != null) {
                        listener.onRemoteDiscovery(discovered);
                    }
                }
            }
        });
    }

    //--------------- URI update and rebalance methods -----------------------//

    public void add(final URI uri) {
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                uris.add(uri);
            }
        });
    }

    public void remove(final URI uri) {
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                uris.remove(uri);
            }
        });
    }

    //--------------- Property Getters and Setters ---------------------------//

    @Override
    public URI getRemoteURI() {
        Provider provider = this.provider;
        if (provider != null) {
            return provider.getRemoteURI();
        }
        return null;
    }

    @Override
    public void setProviderListener(ProviderListener listener) {
        this.listener = listener;
    }

    @Override
    public ProviderListener getProviderListener() {
        return listener;
    }

    public boolean isRandomize() {
        return uris.isRandomize();
    }

    public void setRandomize(boolean value) {
        this.uris.setRandomize(value);
    }

    public long getInitialReconnectDelay() {
        return initialReconnectDelay;
    }

    public void setInitialReconnectDelay(long initialReconnectDelay) {
        this.initialReconnectDelay = initialReconnectDelay;
    }

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(long reconnectDealy) {
        this.reconnectDelay = reconnectDealy;
    }

    public long getMaxReconnectDelay() {
        return maxReconnectDelay;
    }

    public void setMaxReconnectDelay(long maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    public int getStartupMaxReconnectAttempts() {
        return startupMaxReconnectAttempts;
    }

    public void setStartupMaxReconnectAttempts(int startupMaxReconnectAttempts) {
        this.startupMaxReconnectAttempts = startupMaxReconnectAttempts;
    }

    /**
     * Gets the current setting controlling how many Connect / Reconnect attempts must occur
     * before a warn message is logged.  A value of {@literal <= 0} indicates that there will be
     * no warn message logged regardless of how many reconnect attempts occur.
     *
     * @return the current number of connection attempts before warn logging is triggered.
     */
    public int getWarnAfterReconnectAttempts() {
        return warnAfterReconnectAttempts;
    }

    /**
     * Sets the number of Connect / Reconnect attempts that must occur before a warn message
     * is logged indicating that the transport is not connected.  This can be useful when the
     * client is running inside some container or service as it gives an indication of some
     * problem with the client connection that might not otherwise be visible.  To disable the
     * log messages this value should be set to a value {@literal <= 0}
     *
     * @param warnAfterReconnectAttempts
     *        The number of failed connection attempts that must happen before a warning is logged.
     */
    public void setWarnAfterReconnectAttempts(int warnAfterReconnectAttempts) {
        this.warnAfterReconnectAttempts = warnAfterReconnectAttempts;
    }

    public double getReconnectBackOffMultiplier() {
        return reconnectBackOffMultiplier;
    }

    public void setReconnectBackOffMultiplier(double reconnectBackOffMultiplier) {
        this.reconnectBackOffMultiplier = reconnectBackOffMultiplier;
    }

    public boolean isUseReconnectBackOff() {
        return useReconnectBackOff;
    }

    public void setUseReconnectBackOff(boolean useReconnectBackOff) {
        this.useReconnectBackOff = useReconnectBackOff;
    }

    public long getCloseTimeout() {
        return this.closeTimeout;
    }

    public long getSendTimeout() {
        return this.sendTimeout;
    }

    public long getRequestTimeout() {
        return this.requestTimeout;
    }

    public String getAmqpOpenServerListAction() {
        return amqpOpenServerListAction.toString();
    }

    public void setAmqpOpenServerListAction(String amqpOpenServerListAction) {
        this.amqpOpenServerListAction = FailoverServerListAction.valueOf(amqpOpenServerListAction.toUpperCase(Locale.ENGLISH));
    }

    public Map<String, String> getNestedOptions() {
        return uris.getNestedOptions();
    }

    @Override
    public String toString() {
        return "FailoverProvider: " +
               (connectedURI == null ? "unconnected" : connectedURI.toString());
    }

    //--------------- FailoverProvider Request Timeout Support ---------------//

    protected final long getRequestSweeperInterval() {
        long[] timeouts = new long[] { requestTimeout, sendTimeout };

        Arrays.sort(timeouts);

        for (long timeout : timeouts) {
            if (timeout != JmsConnectionInfo.INFINITE) {
                return Math.max(Math.max(1, timeout) / 3, MINIMUM_TIMEOUT);
            }
        }

        return DISABLED;
    }

    protected final class FailoverRequestSweeper implements Runnable {

        @Override
        public void run() {
            List<FailoverRequest> copied = new ArrayList<FailoverRequest>(requests.values());
            for (FailoverRequest request : copied) {
                if (request.isExpired()) {
                    LOG.trace("Task {} has timed out, sending failure notice.", request);
                    request.onFailure(request.createTimedOutException());
                }
            }
        }
    }

    //--------------- FailoverProvider Asynchronous Request ------------------//

    /**
     * For all requests that are dispatched from the FailoverProvider to a connected
     * Provider instance an instance of FailoverRequest is used to handle errors that
     * occur during processing of that request and trigger a reconnect.
     */
    protected abstract class FailoverRequest extends WrappedAsyncResult implements Runnable {

        protected final long id = requestId.incrementAndGet();

        private final long requestStarted = System.nanoTime();
        private final long requestTimeout;

        public FailoverRequest(AsyncResult watcher) {
            this(watcher, JmsConnectionInfo.INFINITE);
        }

        public FailoverRequest(AsyncResult watcher, long requestTimeout) {
            super(watcher);
            this.requestTimeout = requestTimeout;
            LOG.trace("Created Failover Task: {} ({})", this, id);
        }

        @Override
        public void run() {
            requests.put(id, this);
            if (provider == null) {
                whenOffline(new IOException("Connection failed."));
            } else {
                try {
                    LOG.debug("Executing Failover Task: {} ({})", this, id);
                    doTask();
                } catch (UnsupportedOperationException e) {
                    requests.remove(id);
                    getWrappedRequest().onFailure(e);
                } catch (JMSException jmsEx) {
                    requests.remove(id);
                    getWrappedRequest().onFailure(jmsEx);
                } catch (Throwable e) {
                    LOG.debug("Caught exception while executing task: {} - {}", this, e.getMessage());
                    whenOffline(IOExceptionSupport.create(e));
                    handleProviderFailure(IOExceptionSupport.create(e));
                }
            }
        }

        @Override
        public void onFailure(final Throwable error) {
            if (error instanceof JMSException || closingConnection.get() || closed.get() || failed.get()) {
                requests.remove(id);
                super.onFailure(error);
            } else {
                LOG.debug("Request received error: {}", error.getMessage());
                serializer.execute(new Runnable() {
                    @Override
                    public void run() {
                        IOException ioError = IOExceptionSupport.create(error);
                        whenOffline(ioError);
                        handleProviderFailure(ioError);
                    }
                });
            }
        }

        @Override
        public void onSuccess() {
            requests.remove(id);
            super.onSuccess();
        }

        /**
         * Called to execute the specific task that was requested.
         *
         * @throws Exception if an error occurs during task execution.
         */
        public abstract void doTask() throws Exception;

        /**
         * Should the request just succeed when the Provider is not connected.
         *
         * @return true if the request is marked as successful when not connected.
         */
        public boolean succeedsWhenOffline() {
            return false;
        }

        /**
         * When the transport is not connected should this request automatically fail.
         *
         * @return true if the task should fail when the Provider is not connected.
         */
        public boolean failureWhenOffline() {
            return false;
        }

        /**
         * @return true if the request has a configured expiration time.
         */
        public boolean isExpired() {
            if (requestTimeout != JmsConnectionInfo.INFINITE) {
                return (System.nanoTime() - requestStarted) > TimeUnit.MILLISECONDS.toNanos(requestTimeout);
            } else {
                return false;
            }
        }

        protected JMSException createTimedOutException() {
            return new JmsOperationTimedOutException("Timed out waiting on " +  this);
        }

        protected Exception createOfflineFailureException(IOException error) {
            return IOExceptionSupport.create(error);
        }

        private void whenOffline(IOException error) {
            if (failureWhenOffline()) {
                requests.remove(id);
                getWrappedRequest().onFailure(createOfflineFailureException(error));
            } else if (succeedsWhenOffline()) {
                onSuccess();
            } else {
                LOG.trace("Failover task held until connection recovered: {} ({})", this, id);
            }
        }
    }

    /**
     * Captures the initial request to create a JmsConnectionInfo based resources and ensures
     * that if the connection is successfully established that the connection established event
     * is triggered once before moving on to sending only connection interrupted and restored
     * events.
     *
     * The connection state events must all be triggered from the FailoverProvider's serialization
     * thread, this class ensures that the connection established event follows that pattern.
     */
    protected abstract class CreateConnectionRequest extends FailoverRequest {

        public CreateConnectionRequest(AsyncResult watcher) {
            super(watcher);
        }

        @Override
        public void onSuccess() {
            serializer.execute(new Runnable() {
                @Override
                public void run() {
                    if (firstConnection) {
                        LOG.trace("First connection requst has completed:");
                        FailoverProvider.this.messageFactory.set(provider.getMessageFactory());
                        listener.onConnectionEstablished(provider.getRemoteURI());
                        firstConnection = false;
                    } else {
                        LOG.warn("A second call to a CreateConnectionRequest not expected.");
                    }

                    CreateConnectionRequest.this.signalConnected();
                }
            });
        }

        @Override
        public void onFailure(final Throwable result) {
            if (closingConnection.get() || closed.get() || failed.get()) {
                requests.remove(id);
                super.onFailure(result);
            } else {
                LOG.debug("Request received error: {}", result.getMessage());
                serializer.execute(new Runnable() {
                    @Override
                    public void run() {
                        handleProviderFailure(IOExceptionSupport.create(result));
                    }
                });
            }
        }

        public void signalConnected() {
            super.onSuccess();
        }
    }

    private static enum FailoverServerListAction {
        ADD, REPLACE, IGNORE
    };
}
