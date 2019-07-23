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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderFutureFactory;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderSynchronization;
import org.apache.qpid.jms.provider.WrappedAsyncResult;
import org.apache.qpid.jms.provider.exceptions.ProviderClosedException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionRedirectedException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionSecurityException;
import org.apache.qpid.jms.provider.exceptions.ProviderConnectionSecuritySaslException;
import org.apache.qpid.jms.provider.exceptions.ProviderExceptionSupport;
import org.apache.qpid.jms.provider.exceptions.ProviderFailedException;
import org.apache.qpid.jms.provider.exceptions.ProviderIOException;
import org.apache.qpid.jms.provider.exceptions.ProviderOperationTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderSendTimedOutException;
import org.apache.qpid.jms.provider.exceptions.ProviderTransactionRolledBackException;
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

    private static enum FailoverServerListAction {
        ADD, REPLACE, IGNORE
    }

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

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledThreadPoolExecutor serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicBoolean closingConnection = new AtomicBoolean(false);
    private final AtomicLong requestId = new AtomicLong();
    private final Map<Long, FailoverRequest> requests = Collections.synchronizedMap(new LinkedHashMap<Long, FailoverRequest>());
    private final DefaultProviderListener closedListener = new DefaultProviderListener();
    private final AtomicReference<JmsMessageFactory> messageFactory = new AtomicReference<JmsMessageFactory>();
    private final ProviderFutureFactory futureFactory;

    // Current state of connection / reconnection
    private final ReconnectControls reconnectControl = new ReconnectControls();
    private ProviderException failureCause;
    private volatile URI connectedURI;
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

    public FailoverProvider(List<URI> uris, Map<String, String> nestedOptions, ProviderFutureFactory futureFactory) {
        this.uris = new FailoverUriPool(uris, nestedOptions);
        this.futureFactory = futureFactory;

        // All Connection attempts happen in this executor thread as well as handling
        // failed connection events and other maintenance work.
        serializer = new ScheduledThreadPoolExecutor(1, new QpidJMSThreadFactory("FailoverProvider: async work thread", true));
        serializer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        serializer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    @Override
    public void connect(JmsConnectionInfo connectionInfo) throws ProviderException {
        checkClosed();
        this.connectionInfo = connectionInfo;
        LOG.debug("Initiating initial connection attempt task");
        triggerReconnectionAttempt();
    }

    @Override
    public void start() throws ProviderException, IllegalStateException {
        checkClosed();

        if (listener == null) {
            throw new IllegalStateException("No ProviderListener registered.");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            final ProviderFuture request = futureFactory.createFuture();

            serializer.execute(() -> {
                // At this point the closed flag is set and any threads running through this
                // provider will see it as being closed and react accordingly.  Any events
                // that fire from the active provider that is being closed will read a closed
                // state and not trigger new events into the executor. Any event that might be
                // sitting behind this one would also read the closed state and perform no
                // further work while we do a graceful shutdown of the executor service.
                lock.readLock().lock();
                try {
                    ProviderException error = failureCause != null ? failureCause : new ProviderClosedException("Connection closed");
                    final List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                    for (FailoverRequest pendingRequest : pending) {
                        if (!pendingRequest.isComplete()) {
                            pendingRequest.onFailure(error);
                        }
                    }

                    if (requestTimeoutTask != null) {
                        requestTimeoutTask.cancel(false);
                    }

                    if (provider != null) {
                        provider.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error caught while closing Provider: ", e.getMessage());
                } finally {
                    lock.readLock().unlock();
                    request.onSuccess();
                }
            });

            try {
                if (getCloseTimeout() < 0) {
                    request.sync();
                } else {
                    request.sync(getCloseTimeout(), TimeUnit.MILLISECONDS);
                }
            } catch (ProviderException e) {
                LOG.warn("Error caught while closing Provider: {}", e.getMessage() != null ? e.getMessage() : "<Unknown Error>");
            } finally {
                ThreadPoolUtils.shutdownGraceful(serializer);
            }
        }
    }

    @Override
    public void create(final JmsResource resource, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending;
        if (resource instanceof JmsConnectionInfo) {
            pending = new CreateConnectionRequest(request) {
                @Override
                public void doTask(Provider provider) throws ProviderException {
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
                public void doTask(Provider provider) throws ProviderException {
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

        pending.run();
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.start(resource, this);
            }

            @Override
            public String toString() {
                return "start -> " + resource;
            }
        };

        pending.run();
    }

    @Override
    public void stop(final JmsResource resource, final AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.stop(resource, this);
            }

            @Override
            public String toString() {
                return "stop -> " + resource;
            }
        };

        pending.run();
    }

    @Override
    public void destroy(final JmsResource resourceId, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
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

        pending.run();
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, sendTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.send(envelope, this);
            }

            @Override
            public String toString() {
                return "send -> " + envelope;
            }

            @Override
            public ProviderException createTimedOutException() {
                return new ProviderSendTimedOutException("Timed out waiting on " + this, envelope.getMessage());
            }
        };

        pending.run();
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, final ACK_TYPE ackType, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.acknowledge(sessionId, ackType, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acknowledgement would be stale after reconnect.
                return true;
            }

            @Override
            public String toString() {
                return "session acknowledge -> " + sessionId;
            }
        };

        pending.run();
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.acknowledge(envelope, ackType, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acknowledgement would be stale after reconnect.
                return true;
            }

            @Override
            public String toString() {
                return "message acknowledge -> " + envelope + " ackType: " + ackType;
            }
        };

        pending.run();
    }

    @Override
    public void commit(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
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
            protected ProviderException createOfflineFailureException(ProviderException error) {
                return new ProviderTransactionRolledBackException("Commit failed, connection offline: " + error.getMessage(), error);
            }
        };

        pending.run();
    }

    @Override
    public void rollback(final JmsTransactionInfo transactionInfo, JmsTransactionInfo nextTransactionInfo, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
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

        pending.run();
    }

    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
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

        pending.run();
    }

    @Override
    public void unsubscribe(final String subscription, AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request, requestTimeout) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.unsubscribe(subscription, this);
            }

            @Override
            public String toString() {
                return "unsubscribe -> " + subscription;
            }
        };

        pending.run();
    }

    @Override
    public void pull(final JmsConsumerId consumerId, final long timeout, final AsyncResult request) throws ProviderException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask(Provider provider) throws ProviderException {
                provider.pull(consumerId, timeout, this);
            }

            @Override
            public String toString() {
                return "message pull -> " + consumerId;
            }
        };

        pending.run();
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        return messageFactory.get();
    }

    //--------------- Connection Error and Recovery methods ------------------//

    /**
     * This method is always called from within the FailoverProvider's state lock.
     * <p>
     * When a failure is encountered either from an outgoing request or from an error fired
     * from the underlying Provider instance this method is called to determine if a reconnect
     * is allowed and if so a new reconnect cycle is triggered on the serialization executor.
     *
     * @param provider
     * 		  the Provider that was in use when the failure occurred.
     * @param cause
     *        the error that triggered the failure of the provider.
     */
    private void handleProviderFailure(final Provider provider, final ProviderException cause) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        serializer.execute(() -> {
            if (closingConnection.get() || closed.get() || failed.get()) {
                return;
            }

            lock.readLock().lock();
            try {
                // It is possible that another failed request signaled an error for the same provider
                // and we already cleaned up the old failed provider and scheduled a reconnect that
                // has already succeeded, so we need to ensure that we don't kill a valid provider.
                if (provider == FailoverProvider.this.provider) {
                    LOG.debug("handling Provider failure: {}", cause.getMessage());
                    LOG.trace("stack", cause);
                    FailoverProvider.this.provider = null;

                    provider.setProviderListener(closedListener);
                    URI failedURI = provider.getRemoteURI();
                    try {
                        provider.close();
                    } catch (Throwable error) {
                        LOG.trace("Caught exception while closing failed provider: {}", error.getMessage());
                    }

                    if (reconnectControl.isReconnectAllowed(cause)) {
                        if (cause instanceof ProviderConnectionRedirectedException) {
                            ProviderConnectionRedirectedException redirect = (ProviderConnectionRedirectedException) cause;
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

                        final List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                        for (FailoverRequest request : pending) {
                            request.whenOffline(cause);
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
                        failed.set(true);
                        failureCause = cause;
                        ProviderListener listener = this.listener;
                        if (listener != null) {
                            listener.onConnectionFailure(cause);
                        }
                    }
                } else {
                    LOG.trace("Ignoring duplicate provider failed event for provider: {}", provider);
                }
            } finally {
                lock.readLock().unlock();
            }
        });
    }

    /**
     * Called from the serialization executor after a successful connection.
     * <p>
     * This method enqueues a new task that will attempt to recover connection state.
     * Once successfully recovered, normal operations will resume.  If an error occurs
     * while attempting to recover the JMS framework state then a reconnect cycle is again
     * triggered on the connection thread.
     *
     * @param provider
     *        The newly connect Provider instance that will become active.
     */
    private void initializeNewConnection(final Provider provider) {
        serializer.execute(() -> {
            // Disallow other processing in the provider while we attempt to establish this
            // provider as the new one for recovery, any incoming work stops until we finish
            // and either recover or go back into a failed state.
            lock.writeLock().lock();

            try {
                // In case a close is in play as we are reconnecting we close out the connected
                // provider instance and return here to allow any pending close operations to
                // finish now.
                if (closingConnection.get() || closed.get() || failed.get()) {
                    try {
                        provider.close();
                    } catch(Throwable ignore) {
                        LOG.trace("Ingoring failure to close failed provider: {}", provider, ignore);
                    }

                    return;
                }

                FailoverProvider.this.provider = provider;
                provider.setProviderListener(FailoverProvider.this);
                connectedURI = provider.getRemoteURI();

                if (reconnectControl.isRecoveryRequired()) {
                    LOG.debug("Signalling connection recovery: {}", provider);

                    // Stage 1: Allow listener to recover its resources
                    try {
                        listener.onConnectionRecovery(provider);
                    } finally {
                        // Stage 2: If the provider knows of others lets add them to the URI pool
                        //          even if something failed here we can learn of new hosts so we
                        //          always process the potential Open frame failover URI results.
                        processAlternates(provider.getAlternateURIs());
                    }

                    // Stage 3: Connection state recovered, get newly configured message factory.
                    FailoverProvider.this.messageFactory.set(provider.getMessageFactory());

                    // Stage 4: Restart consumers, send pull commands, etc.
                    listener.onConnectionRecovered(provider);

                    // Stage 5: Let the client know that connection has restored.
                    listener.onConnectionRestored(provider.getRemoteURI());

                    // Last step: Send pending actions.
                    final List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                    for (FailoverRequest request : pending) {
                        if (!request.isComplete()) {
                            request.run();
                        }
                    }

                    reconnectControl.connectionEstablished();
                } else {
                    processAlternates(provider.getAlternateURIs());

                    // Last step: Send pending actions.
                    final List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                    for (FailoverRequest request : pending) {
                        if (!request.isComplete()) {
                            request.run();
                        }
                    }
                }

                // Cancel timeout processing since we are connected again.  We waited until
                // now for the case where we are continually getting bounced from otherwise
                // live servers, we want the timeout to remain scheduled in that case so that
                // it doesn't keep getting rescheduled and never actually time anything out.
                if (requestTimeoutTask != null) {
                    requestTimeoutTask.cancel(false);
                    requestTimeoutTask = null;
                }
            } catch (Throwable error) {
                LOG.trace("Connection attempt:[{}] to: {} failed", reconnectControl.reconnectAttempts, provider.getRemoteURI());
                handleProviderFailure(provider, ProviderExceptionSupport.createOrPassthroughFatal(error));
            } finally {
                lock.writeLock().unlock();
            }
        });
    }

    /**
     * Called when the Provider was either first created or when a connection failure has
     * been reported.  A reconnection attempt is executed by the serialization executor either
     * immediately or after a delay based on configuration and number of attempts that have
     * elapsed.  If a new Provider is able to be created and connected then a recovery task
     * is scheduled with the serialization executor.  If the connect attempt fails another
     * attempt is scheduled based on the configured delay settings until a max attempts
     * limit is hit, if one is set.
     */
    private void triggerReconnectionAttempt() {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        reconnectControl.scheduleReconnect(new Runnable() {

            @Override
            public void run() {
                if (provider != null || closingConnection.get() || closed.get() || failed.get()) {
                    return;
                }

                ProviderException failure = null;
                Provider provider = null;

                long reconnectAttempts = reconnectControl.recordNextAttempt();

                try {
                    if (!uris.isEmpty()) {
                        for (int i = 0; i < uris.size(); ++i) {
                            URI target = uris.getNext();
                            if (target == null) {
                                LOG.trace("Failover URI collection unexpectedly modified during connection attempt.");
                                failure = ProviderExceptionSupport.createOrPassthroughFatal(
                                    new ConcurrentModificationException("Failover URIs changed unexpectedly"));
                                continue;
                            }

                            try {
                                LOG.debug("Connection attempt:[{}] to: {} in-progress", reconnectAttempts,
                                    target.getScheme() + "://" + target.getHost() + ":" + target.getPort());
                                provider = ProviderFactory.create(target, futureFactory);
                                provider.connect(connectionInfo);
                                initializeNewConnection(provider);
                                return;
                            } catch (Throwable e) {
                                LOG.info("Connection attempt:[{}] to: {} failed", reconnectAttempts,
                                    target.getScheme() + "://" + target.getHost() + ":" + target.getPort());
                                failure = ProviderExceptionSupport.createOrPassthroughFatal(e);
                                try {
                                    if (provider != null) {
                                        provider.close();
                                    }
                                } catch (Throwable ex) {
                                } finally {
                                    provider = null;
                                }

                                if (reconnectControl.isStoppageCause(failure)) {
                                    LOG.trace("Stopping attempt due to type of failure");
                                    break;
                                }
                            }
                        }
                    } else {
                        LOG.debug("No remote URI available to connect to in failover list");
                        failure = new ProviderFailedException(
                            "No remote URI available for reconnection during connection attempt: " + reconnectAttempts);
                    }
                } catch (Throwable unknownFailure) {
                    LOG.warn("Connection attempt:[{}] failed abnormally.", reconnectAttempts);
                    failure = failure == null ? ProviderExceptionSupport.createOrPassthroughFatal(unknownFailure) : failure;
                } finally {
                    if (provider == null) {
                        LOG.trace("Connection attempt:[{}] failed error: {}", reconnectControl.reconnectAttempts, failure.getMessage());
                        if (!reconnectControl.isReconnectAllowed(failure)) {
                            reportReconnectFailure(failure);
                        } else {
                            reconnectControl.scheduleReconnect(this);
                        }
                    }
                }
            }
        });
    }

    /**
     * Called when the provider has tried to reconnect for the last time based on reconnection policy
     * configuration and we now consider this connection attempt to be failed.
     *
     * @param lastFailure the last failure encountered while trying to (re)connect.
     */
    private void reportReconnectFailure(final ProviderException lastFailure) {
        serializer.execute(() -> {
            LOG.error("Failed to connect after: " + reconnectControl.reconnectAttempts + " attempt(s)");
            if (failed.compareAndSet(false, true)) {
                if (lastFailure == null) {
                    failureCause = new ProviderIOException(
                        "Failed to connect after: " + reconnectControl.reconnectAttempts + " attempt(s)");
                } else {
                    failureCause = ProviderExceptionSupport.createOrPassthroughFatal(lastFailure);
                }
                if (listener != null) {
                    listener.onConnectionFailure(failureCause);
                };
            }
        });
    }

    protected void checkClosed() throws ProviderException {
        if (closed.get()) {
            throw new ProviderClosedException("The Provider is already closed");
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
    public void onFailedMessageSend(final JmsOutboundMessageDispatch envelope, ProviderException cause) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        listener.onFailedMessageSend(envelope, cause);
    }

    @Override
    public void onConnectionFailure(final ProviderException ex) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        LOG.debug("Failover: the provider reports failure: {}", ex.getMessage());
        handleProviderFailure(provider, ex);
    }

    @Override
    public void onResourceClosed(JmsResource resource, ProviderException cause) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        listener.onResourceClosed(resource, cause);
    }

    @Override
    public void onProviderException(final ProviderException ex) {
        if (closingConnection.get() || closed.get() || failed.get()) {
            return;
        }

        LOG.debug("Provider reports an async error: {}", ex.getMessage());
        listener.onProviderException(ex);
    }

    //--------------- Processing for server-provided alternate URIs  ---------//

    private void processAlternates(List<URI> alternates) {
        if (!alternates.isEmpty()) {
            List<URI> newRemotes = new ArrayList<URI>(alternates);

            LOG.debug("Processing alternates uris:{} with new set: {}", uris, newRemotes);

            switch (amqpOpenServerListAction) {
                case ADD:
                    try {
                        uris.addAll(alternates);
                    } catch (Throwable err) {
                        LOG.warn("Error while attempting to add discovered URIs: {}", alternates);
                    }
                    break;
                case REPLACE:
                    // The current server is assumed not to be in the list of updated remote
                    // as it is meant for the failover nodes. The pool will de-dup if it is.
                    newRemotes.add(0, connectedURI);
                    try {
                        LOG.debug("Replacing uris:{} with new set: {}", uris, newRemotes);
                        uris.replaceAll(newRemotes);
                    } catch (Throwable err) {
                        LOG.warn("Error while attempting to add discovered URIs: {}", alternates);
                    }
                    break;
                case IGNORE:
                    // Do Nothing
                    break;
                default:
                    // Shouldn't get here, but do nothing if we do.
                    break;
            }

            LOG.debug("Processing alternates done new uris:{}", uris);
        }
    }

    //--------------- URI update and rebalance methods -----------------------//

    public void add(final URI uri) {
        serializer.execute(() -> uris.add(uri));
    }

    public void remove(final URI uri) {
        serializer.execute(() -> uris.remove(uri));
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
    public List<URI> getAlternateURIs() {
        Provider provider = this.provider;
        if (provider != null) {
            return provider.getAlternateURIs();
        }
        return null;
    };

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
    public ProviderFuture newProviderFuture() {
        return futureFactory.createFuture();
    }

    @Override
    public ProviderFuture newProviderFuture(ProviderSynchronization synchronization) {
        return futureFactory.createFuture(synchronization);
    }

    @Override
    public String toString() {
        return "FailoverProvider: " + (connectedURI == null ? "unconnected" : connectedURI.toString());
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
            lock.readLock().lock();
            try {
                final List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                for (FailoverRequest request : pending) {
                    if (request.isExpired()) {
                        LOG.trace("Task {} has timed out, sending failure notice.", request);
                        request.onFailure(request.createTimedOutException());
                    }
                }
            } finally {
                lock.readLock().unlock();
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

        protected Provider activeProvider;

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
            lock.readLock().lock();
            try {
                // Snapshot the current provider as this action is scoped to that
                // instance and any failure we report should reflect the provider
                // that was in use when the failure happened.
                activeProvider = provider;
                requests.put(id, this);
                if (activeProvider == null) {
                    whenOffline(new ProviderIOException("Connection failed."));
                } else {
                    try {
                        LOG.debug("Executing Failover Task: {} ({})", this, id);
                        doTask(activeProvider);
                    } catch (Throwable e) {
                        LOG.debug("Caught exception while executing task: {} - {}", this, e.getMessage());
                        ProviderException providerEx = ProviderExceptionSupport.createNonFatalOrPassthrough(e);
                        whenOffline(providerEx);
                        handleProviderFailure(activeProvider, providerEx);
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void onFailure(final ProviderException error) {
            lock.readLock().lock();
            try {
                if (!(error instanceof ProviderIOException) || closingConnection.get() || closed.get() || failed.get()) {
                    requests.remove(id);
                    super.onFailure(error);
                } else {
                    LOG.debug("Request received error: {}", error.getMessage());
                    ProviderException ioError = ProviderExceptionSupport.createOrPassthroughFatal(error);
                    whenOffline(ioError);
                    handleProviderFailure(activeProvider, ioError);
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void onSuccess() {
            lock.readLock().lock();
            try {
                requests.remove(id);
            } finally {
                lock.readLock().unlock();
            }
            super.onSuccess();
        }

        /**
         * Called to execute the specific task that was requested.
         *
         * @param provider
         * 		The provider instance to use when performing this action.
         *
         * @throws ProviderException if an error occurs during task execution.
         */
        public abstract void doTask(Provider provider) throws ProviderException;

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

        protected ProviderException createTimedOutException() {
            return new ProviderOperationTimedOutException("Timed out waiting on " +  this);
        }

        protected ProviderException createOfflineFailureException(ProviderException error) {
            return ProviderExceptionSupport.createNonFatalOrPassthrough(error);
        }

        private void whenOffline(ProviderException error) {
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
     */
    protected abstract class CreateConnectionRequest extends FailoverRequest {

        public CreateConnectionRequest(AsyncResult watcher) {
            super(watcher);
        }

        @Override
        public void onSuccess() {
            lock.readLock().lock();
            try {
                LOG.trace("First connection requst has completed:");
                FailoverProvider.this.messageFactory.set(provider.getMessageFactory());
                processAlternates(provider.getAlternateURIs());
                listener.onConnectionEstablished(provider.getRemoteURI());
                reconnectControl.connectionEstablished();
                CreateConnectionRequest.this.signalConnected();
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void onFailure(final ProviderException result) {
            lock.readLock().lock();
            try {
                if (closingConnection.get() || closed.get() || failed.get()) {
                    requests.remove(id);
                    super.onFailure(result);
                } else {
                    LOG.debug("Request received error: {}", result.getMessage());
                    // If we managed to receive an Open frame it might contain
                    // a failover update so process it before handling the error.
                    processAlternates(provider.getAlternateURIs());
                    handleProviderFailure(activeProvider, ProviderExceptionSupport.createOrPassthroughFatal(result));
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        public void signalConnected() {
            super.onSuccess();
        }
    }

    //----- Reconnection Control State Management ----------------------------//

    private class ReconnectControls {

        // Reconnection state tracking
        private volatile boolean recoveryRequired;
        private volatile long reconnectAttempts;
        private volatile long nextReconnectDelay = -1;

        public void scheduleReconnect(Runnable runnable) {
            try {
                // Warn of ongoing connection attempts if configured.
                int warnInterval = getWarnAfterReconnectAttempts();
                if (reconnectAttempts > 0 && warnInterval > 0 && (reconnectAttempts % warnInterval) == 0) {
                    LOG.warn("Failed to connect after: {} attempt(s) continuing to retry.", reconnectAttempts);
                }

                // If no connection recovery required then we have never fully connected to a remote
                // so we proceed down the connect with one immediate connection attempt and then follow
                // on delayed attempts based on configuration.
                if (!recoveryRequired) {
                    if (reconnectAttempts == 0) {
                        LOG.trace("Initial connect attempt will be performed immediately");
                        serializer.execute(runnable);
                    } else if (reconnectAttempts == 1 && initialReconnectDelay > 0) {
                        LOG.trace("Delayed initial reconnect attempt will be in {} milliseconds", initialReconnectDelay);
                        serializer.schedule(runnable, initialReconnectDelay, TimeUnit.MILLISECONDS);
                    } else {
                        long delay = reconnectControl.nextReconnectDelay();
                        LOG.trace("Next reconnect attempt will be in {} milliseconds", delay);
                        serializer.schedule(runnable, delay, TimeUnit.MILLISECONDS);
                    }
                } else if (reconnectAttempts == 0) {
                    if (initialReconnectDelay > 0) {
                        LOG.trace("Delayed initial reconnect attempt will be in {} milliseconds", initialReconnectDelay);
                        serializer.schedule(runnable, initialReconnectDelay, TimeUnit.MILLISECONDS);
                    } else {
                        LOG.trace("Initial Reconnect attempt will be performed immediately");
                        serializer.execute(runnable);
                    }
                } else {
                    long delay = reconnectControl.nextReconnectDelay();
                    LOG.trace("Next reconnect attempt will be in {} milliseconds", delay);
                    serializer.schedule(runnable, delay, TimeUnit.MILLISECONDS);
                }
            } catch (Throwable unrecoverable) {
                reportReconnectFailure(ProviderExceptionSupport.createOrPassthroughFatal(unrecoverable));
            }
        }

        public void connectionEstablished() {
            recoveryRequired = true;
            nextReconnectDelay = -1;
            reconnectAttempts = 0;
            uris.connected();
        }

        public long recordNextAttempt() {
            return ++reconnectAttempts;
        }

        public boolean isRecoveryRequired() {
            return recoveryRequired;
        }

        public boolean isLimitExceeded() {
            int reconnectLimit = reconnectAttemptLimit();
            if (reconnectLimit != UNLIMITED && reconnectAttempts >= reconnectLimit) {
                return true;
            }

            return false;
        }

        public boolean isReconnectAllowed(ProviderException cause) {
            // If a connection attempts fail due to Security errors than we abort
            // reconnection as there is a configuration issue and we want to avoid
            // a spinning reconnect cycle that can never complete.
            if (isStoppageCause(cause)) {
                return false;
            }

            return !isLimitExceeded();
        }

        private boolean isStoppageCause(ProviderException cause) {
            if (cause instanceof ProviderConnectionSecuritySaslException) {
                ProviderConnectionSecuritySaslException saslFailure = (ProviderConnectionSecuritySaslException) cause;
                return !saslFailure.isSysTempFailure();
            } else if (cause instanceof ProviderConnectionSecurityException ) {
                return true;
            }

            return false;
        }

        private int reconnectAttemptLimit() {
            int maxReconnectValue = maxReconnectAttempts;
            if (!recoveryRequired && startupMaxReconnectAttempts != UNDEFINED) {
                // If this is the first connection attempt and a specific startup retry limit
                // is configured then use it, otherwise use the main reconnect limit
                maxReconnectValue = startupMaxReconnectAttempts;
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
    }
}
