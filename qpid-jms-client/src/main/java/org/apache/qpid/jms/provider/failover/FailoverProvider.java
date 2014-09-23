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
package org.apache.qpid.jms.provider.failover;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.JMSException;

import org.apache.qpid.jms.JmsSslContext;
import org.apache.qpid.jms.message.JmsDefaultMessageFactory;
import org.apache.qpid.jms.message.JmsInboundMessageDispatch;
import org.apache.qpid.jms.message.JmsMessageFactory;
import org.apache.qpid.jms.message.JmsOutboundMessageDispatch;
import org.apache.qpid.jms.meta.JmsConnectionInfo;
import org.apache.qpid.jms.meta.JmsConsumerId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsSessionId;
import org.apache.qpid.jms.provider.AsyncResult;
import org.apache.qpid.jms.provider.DefaultProviderListener;
import org.apache.qpid.jms.provider.Provider;
import org.apache.qpid.jms.provider.ProviderFactory;
import org.apache.qpid.jms.provider.ProviderFuture;
import org.apache.qpid.jms.provider.ProviderListener;
import org.apache.qpid.jms.provider.ProviderConstants.ACK_TYPE;
import org.apache.qpid.jms.util.IOExceptionSupport;
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

    private static final int UNLIMITED = -1;

    private ProviderListener listener;
    private Provider provider;
    private final FailoverUriPool uris;

    private final ExecutorService serializer;
    private final ScheduledExecutorService connectionHub;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicLong requestId = new AtomicLong();
    private final Map<Long, FailoverRequest> requests = new LinkedHashMap<Long, FailoverRequest>();
    private final DefaultProviderListener closedListener = new DefaultProviderListener();
    private final JmsSslContext sslContext;
    private final JmsMessageFactory defaultMessageFactory = new JmsDefaultMessageFactory();

    // Current state of connection / reconnection
    private boolean firstConnection = true;
    private long reconnectAttempts;
    private long reconnectDelay = TimeUnit.SECONDS.toMillis(5);
    private IOException failureCause;
    private URI connectedURI;

    // Timeout values configured via JmsConnectionInfo
    private long connectTimeout = JmsConnectionInfo.DEFAULT_CONNECT_TIMEOUT;
    private long closeTimeout = JmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;
    private long sendTimeout =  JmsConnectionInfo.DEFAULT_SEND_TIMEOUT;
    private long requestTimeout = JmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;

    // Configuration values.
    private long initialReconnectDelay = 0L;
    private long maxReconnectDelay = TimeUnit.SECONDS.toMillis(30);
    private boolean useExponentialBackOff = true;
    private double backOffMultiplier = 2d;
    private int maxReconnectAttempts = UNLIMITED;
    private int startupMaxReconnectAttempts = UNLIMITED;
    private int warnAfterReconnectAttempts = 10;

    public FailoverProvider(Map<String, String> nestedOptions) {
        this(null, nestedOptions);
    }

    public FailoverProvider(URI[] uris) {
        this(uris, null);
    }

    public FailoverProvider(URI[] uris, Map<String, String> nestedOptions) {
        this.uris = new FailoverUriPool(uris, nestedOptions);
        this.sslContext = JmsSslContext.getCurrentSslContext();

        this.serializer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName("FailoverProvider: serialization thread");
                return serial;
            }
        });

        // All Connection attempts happen in this schedulers thread.  Once a connection
        // is established it will hand the open connection back to the serializer thread
        // for state recovery.
        this.connectionHub = Executors.newScheduledThreadPool(1, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runner) {
                Thread serial = new Thread(runner);
                serial.setDaemon(true);
                serial.setName("FailoverProvider: connect thread");
                return serial;
            }
        });
    }

    @Override
    public void connect() throws IOException {
        checkClosed();
        LOG.debug("Performing initial connection attempt");
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

                        if (provider != null) {
                            provider.close();
                        }
                    } catch (Exception e) {
                        LOG.debug("Caught exception while closing connection");
                    } finally {

                        if (connectionHub != null) {
                            connectionHub.shutdown();
                        }

                        if (serializer != null) {
                            serializer.shutdown();
                        }

                        request.onSuccess();
                    }
                }
            });

            try {
                if (this.closeTimeout >= 0) {
                    request.sync();
                } else {
                    request.sync(closeTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                LOG.warn("Error caught while closing Provider: ", e.getMessage());
            }
        }
    }

    @Override
    public void create(final JmsResource resource, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                if (resource instanceof JmsConnectionInfo) {
                    JmsConnectionInfo connectionInfo = (JmsConnectionInfo) resource;
                    connectTimeout = connectionInfo.getConnectTimeout();
                    closeTimeout = connectionInfo.getCloseTimeout();
                    sendTimeout = connectionInfo.getSendTimeout();
                    requestTimeout = connectionInfo.getRequestTimeout();
                }

                provider.create(resource, this);
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void start(final JmsResource resource, final AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.start(resource, this);
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void destroy(final JmsResource resourceId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws IOException, JMSException, UnsupportedOperationException {
                provider.destroy(resourceId, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acks would be stale.
                return true;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void send(final JmsOutboundMessageDispatch envelope, AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.send(envelope, this);
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void acknowledge(final JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.acknowledge(sessionId, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acks would be stale.
                return true;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void acknowledge(final JmsInboundMessageDispatch envelope, final ACK_TYPE ackType, AsyncResult request) throws IOException, JMSException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.acknowledge(envelope, ackType, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                // Allow this to succeed, acks would be stale.
                return true;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void commit(final JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.commit(sessionId, this);
            }

            @Override
            public boolean failureWhenOffline() {
                return true;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void rollback(final JmsSessionId sessionId, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.rollback(sessionId, this);
            }

            @Override
            public boolean failureWhenOffline() {
                return true;
            }
        };

        serializer.execute(pending);
    }


    @Override
    public void recover(final JmsSessionId sessionId, final AsyncResult request) throws IOException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.recover(sessionId, this);
            }

            @Override
            public boolean succeedsWhenOffline() {
                return true;
            }
        };

        serializer.execute(pending);
    }

    @Override
    public void unsubscribe(final String subscription, AsyncResult request) throws IOException, JMSException, UnsupportedOperationException {
        checkClosed();
        final FailoverRequest pending = new FailoverRequest(request) {
            @Override
            public void doTask() throws Exception {
                provider.unsubscribe(subscription, this);
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
        };

        serializer.execute(pending);
    }

    @Override
    public JmsMessageFactory getMessageFactory() {
        final AtomicReference<JmsMessageFactory> result =
            new AtomicReference<JmsMessageFactory>(defaultMessageFactory);

        serializer.execute(new Runnable() {

            @Override
            public void run() {
                if (provider != null) {
                    result.set(provider.getMessageFactory());
                }
            }
        });

        return result.get();
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
     */
    private void handleProviderFailure(final IOException cause) {
        LOG.debug("handling Provider failure: {}", cause.getMessage());
        LOG.trace("stack", cause);

        this.provider.setProviderListener(closedListener);
        URI failedURI = this.provider.getRemoteURI();
        try {
            this.provider.close();
        } catch (Throwable error) {
            LOG.trace("Caught exception while closing failed provider: {}", error.getMessage());
        }
        this.provider = null;

        if (reconnectAllowed()) {
            ProviderListener listener = this.listener;
            if (listener != null) {
                listener.onConnectionInterrupted(failedURI);
            }
            triggerReconnectionAttempt();
        } else {
            ProviderListener listener = this.listener;
            if (listener != null) {
                listener.onConnectionFailure(cause);
            }
        }
    }

    /**
     * Called from the reconnection thread.  This method enqueues a new task that
     * will attempt to recover connection state, once successful normal operations
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
                    if (firstConnection) {
                        firstConnection = false;
                        FailoverProvider.this.provider = provider;
                        provider.setProviderListener(FailoverProvider.this);

                        List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                        for (FailoverRequest request : pending) {
                            request.run();
                        }
                    } else {
                        LOG.debug("Signalling connection recovery: {}", provider);
                        FailoverProvider.this.provider = provider;
                        provider.setProviderListener(FailoverProvider.this);

                        // Stage 1: Recovery all JMS Framework resources
                        listener.onConnectionRecovery(provider);

                        // Stage 2: Restart consumers, send pull commands, etc.
                        listener.onConnectionRecovered(provider);

                        // Stage 3: Let the client know that connection has restored.
                        listener.onConnectionRestored(provider.getRemoteURI());

                        // Stage 4: Send pending actions.
                        List<FailoverRequest> pending = new ArrayList<FailoverRequest>(requests.values());
                        for (FailoverRequest request : pending) {
                            request.run();
                        }

                        reconnectDelay = initialReconnectDelay;
                        reconnectAttempts = 0;
                        connectedURI = provider.getRemoteURI();
                        uris.connected();
                    }
                } catch (Throwable error) {
                    handleProviderFailure(IOExceptionSupport.create(error));
                }
            }
        });
    }

    /**
     * Called when the Provider was either first created or when a connection failure has
     * been connected.  A reconnection attempt is immediately executed on the connection
     * thread.  If a new Provider is able to be created and connected then a recovery task
     * is scheduled on the main serializer thread.  If the connect attempt fails another
     * attempt is scheduled based on the configured delay settings until a max attempts
     * limit is hit, if one is set.
     */
    private void triggerReconnectionAttempt() {
        if (closed.get() || failed.get()) {
            return;
        }

        connectionHub.execute(new Runnable() {
            @Override
            public void run() {
                if (provider != null || closed.get() || failed.get()) {
                    return;
                }

                reconnectAttempts++;
                Throwable failure = null;
                URI target = uris.getNext();
                if (target != null) {
                    try {
                        LOG.debug("Attempting connection to: {}", target);
                        JmsSslContext.setCurrentSslContext(sslContext);
                        Provider provider = ProviderFactory.createAsync(target);
                        initializeNewConnection(provider);
                        return;
                    } catch (Throwable e) {
                        LOG.info("Connection attempt to: {} failed.", target);
                        failure = e;
                    }
                }

                int reconnectLimit = reconnectAttemptLimit();

                if (reconnectLimit != UNLIMITED && reconnectAttempts >= reconnectLimit) {
                    LOG.error("Failed to connect after: " + reconnectAttempts + " attempt(s)");
                    failed.set(true);
                    failureCause = IOExceptionSupport.create(failure);
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
                connectionHub.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        });
    }

    private boolean reconnectAllowed() {
        return reconnectAttemptLimit() != 0;
    }

    private int reconnectAttemptLimit() {
        int maxReconnectValue = this.maxReconnectAttempts;
        if (firstConnection && this.startupMaxReconnectAttempts != UNLIMITED) {
            maxReconnectValue = this.startupMaxReconnectAttempts;
        }
        return maxReconnectValue;
    }

    private long nextReconnectDelay() {
        if (useExponentialBackOff) {
            // Exponential increment of reconnect delay.
            reconnectDelay *= backOffMultiplier;
            if (reconnectDelay > maxReconnectDelay) {
                reconnectDelay = maxReconnectDelay;
            }
        }

        return reconnectDelay;
    }

    protected void checkClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("The Provider is already closed");
        }
    }

    //--------------- DefaultProviderListener overrides ----------------------//

    @Override
    public void onMessage(final JmsInboundMessageDispatch envelope) {
        if (closed.get() || failed.get()) {
            return;
        }
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closed.get()) {
                    listener.onMessage(envelope);
                }
            }
        });
    }

    @Override
    public void onConnectionFailure(final IOException ex) {
        if (closed.get() || failed.get()) {
            return;
        }
        serializer.execute(new Runnable() {
            @Override
            public void run() {
                if (!closed.get() && !failed.get()) {
                    LOG.debug("Failover: the provider reports failure: {}", ex.getMessage());
                    handleProviderFailure(ex);
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

    public long getInitialReconnectDealy() {
        return initialReconnectDelay;
    }

    public void setInitialReconnectDealy(long initialReconnectDealy) {
        this.initialReconnectDelay = initialReconnectDealy;
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
     * before a warn message is logged.  A value of {@code <= 0} indicates that there will be
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
     * log messages this value should be set to a value @{code attempts <= 0}
     *
     * @param warnAfterReconnectAttempts
     *        The number of failed connection attempts that must happen before a warning is logged.
     */
    public void setWarnAfterReconnectAttempts(int warnAfterReconnectAttempts) {
        this.warnAfterReconnectAttempts = warnAfterReconnectAttempts;
    }

    public double getReconnectDelayExponent() {
        return backOffMultiplier;
    }

    public void setReconnectDelayExponent(double reconnectDelayExponent) {
        this.backOffMultiplier = reconnectDelayExponent;
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    public long getConnectTimeout() {
        return this.connectTimeout;
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

    @Override
    public String toString() {
        return "FailoverProvider: " +
               (connectedURI == null ? "unconnected" : connectedURI.toString());
    }

    //--------------- FailoverProvider Asynchronous Request --------------------//

    /**
     * For all requests that are dispatched from the FailoverProvider to a connected
     * Provider instance an instance of FailoverRequest is used to handle errors that
     * occur during processing of that request and trigger a reconnect.
     *
     * @param <T>
     */
    protected abstract class FailoverRequest extends ProviderFuture implements Runnable {

        private final long id = requestId.incrementAndGet();

        public FailoverRequest(AsyncResult watcher) {
            super(watcher);
        }

        @Override
        public void run() {
            requests.put(id, this);
            if (provider == null) {
                if (failureWhenOffline()) {
                    requests.remove(id);
                    watcher.onFailure(new IOException("Provider disconnected"));
                } else if (succeedsWhenOffline()) {
                    onSuccess();
                }
            } else {
                try {
                    LOG.debug("Executing Failover Task: {}", this);
                    doTask();
                } catch (UnsupportedOperationException e) {
                    requests.remove(id);
                    watcher.onFailure(e);
                } catch (Exception e) {
                    // TODO Should we let JMSException through?
                    LOG.debug("Caught exception while executing task: {}", e.getMessage());
                    triggerReconnectionAttempt();
                }
            }
        }

        @Override
        public void onFailure(final Throwable result) {
            if (closed.get() || failed.get()) {
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
    }
}
