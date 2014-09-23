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
package org.apache.qpid.jms.transports;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;

import org.apache.qpid.jms.util.IOExceptionSupport;
import org.apache.qpid.jms.util.InetAddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.buffer.Buffer;

/**
 *
 */
public class RawTcpTransport implements Transport, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RawTcpTransport.class);

    private TransportListener listener;
    private final URI remoteLocation;
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> connectionError = new AtomicReference<Throwable>();

    private final Socket socket;
    private DataOutputStream dataOut;
    private DataInputStream dataIn;
    private Thread runner;

    private boolean closeAsync = true;
    private int socketBufferSize = 64 * 1024;
    private int soTimeout = 0;
    private int soLinger = Integer.MIN_VALUE;
    private Boolean keepAlive;
    private Boolean tcpNoDelay = true;
    private boolean useLocalHost = false;
    private int ioBufferSize = 8 * 1024;

    /**
     * Create a new instance of the transport.
     *
     * @param listener
     *        The TransportListener that will receive data from this Transport instance.
     * @param remoteLocation
     *        The remote location where this transport should connection to.
     */
    public RawTcpTransport(TransportListener listener, URI remoteLocation) {
        this.listener = listener;
        this.remoteLocation = remoteLocation;

        Socket temp = null;
        try {
            temp = createSocketFactory().createSocket();
        } catch (IOException e) {
            connectionError.set(e);
        }

        this.socket = temp;
    }

    @Override
    public void connect() throws IOException {
        if (connectionError.get() != null) {
            throw IOExceptionSupport.create(connectionError.get());
        }

        if (socket == null) {
            throw new IllegalStateException("Cannot connect if the socket or socketFactory have not been set");
        }

        InetSocketAddress remoteAddress = null;

        if (remoteLocation != null) {
            String host = resolveHostName(remoteLocation.getHost());
            remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
        }

        socket.connect(remoteAddress);

        connected.set(true);

        initialiseSocket(socket);
        initializeStreams();

        runner = new Thread(null, this, "QpidJMS RawTcpTransport: " + toString());
        runner.setDaemon(false);
        runner.start();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (socket == null) {
                return;
            }

            // Closing the streams flush the sockets before closing.. if the socket
            // is hung.. then this hangs the close so we support an asynchronous close
            // by default which will timeout if the close doesn't happen after a delay.
            if (closeAsync) {
                final CountDownLatch latch = new CountDownLatch(1);

                final ExecutorService closer = Executors.newSingleThreadExecutor();
                closer.execute(new Runnable() {
                    @Override
                    public void run() {
                        LOG.trace("Closing socket {}", socket);
                        try {
                            socket.close();
                            LOG.debug("Closed socket {}", socket);
                        } catch (IOException e) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Caught exception closing socket " + socket + ". This exception will be ignored.", e);
                            }
                        } finally {
                            latch.countDown();
                        }
                    }
                });

                try {
                    latch.await(1,TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    closer.shutdownNow();
                }
            } else {
                LOG.trace("Closing socket {}", socket);
                try {
                    socket.close();
                    LOG.debug("Closed socket {}", socket);
                } catch (IOException e) {
                    LOG.debug("Caught exception closing socket {}. This exception will be ignored.", socket, e);
                }
            }
        }
    }

    @Override
    public void send(ByteBuffer output) throws IOException {
        checkConnected();
        LOG.info("RawTcpTransport sending packet of size: {}", output.remaining());
        if (dataOut instanceof OutputStream) {
            WritableByteChannel channel = Channels.newChannel(dataOut);
            channel.write(output);
        } else {
            while (output.hasRemaining()) {
                dataOut.writeByte(output.get());
            }
        }
        dataOut.flush();
    }

    @Override
    public void send(org.fusesource.hawtbuf.Buffer output) throws IOException {
        checkConnected();
        send(output.toByteBuffer());
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    @Override
    public TransportListener getTransportListener() {
        return this.listener;
    }

    @Override
    public void setTransportListener(TransportListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be set to null");
        }

        this.listener = listener;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(Boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getSoLinger() {
        return soLinger;
    }

    public void setSoLinger(int soLinger) {
        this.soLinger = soLinger;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isUseLocalHost() {
        return useLocalHost;
    }

    public void setUseLocalHost(boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
    }

    public int getIoBufferSize() {
        return ioBufferSize;
    }

    public void setIoBufferSize(int ioBufferSize) {
        this.ioBufferSize = ioBufferSize;
    }

    public boolean isCloseAsync() {
        return closeAsync;
    }

    public void setCloseAsync(boolean closeAsync) {
        this.closeAsync = closeAsync;
    }

    //---------- Transport internal implementation ---------------------------//

    @Override
    public void run() {
        LOG.trace("TCP consumer thread for " + this + " starting");
        try {
            while (isConnected()) {
                doRun();
            }
        } catch (IOException e) {
            connectionError.set(e);
            onException(e);
        } catch (Throwable e) {
            IOException ioe = new IOException("Unexpected error occured: " + e);
            connectionError.set(ioe);
            ioe.initCause(e);
            onException(ioe);
        }
    }

    protected void doRun() throws IOException {
        int size = dataIn.available();
        if (size <= 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(1);
            } catch (InterruptedException e) {
            }
            return;
        }

        byte[] buffer = new byte[size];
        dataIn.readFully(buffer);
        Buffer incoming = new Buffer(buffer);
        listener.onData(incoming);
    }

    /**
     * Passes any IO exceptions into the transport listener
     */
    public void onException(IOException e) {
        if (listener != null) {
            try {
                listener.onTransportError(e);
            } catch (RuntimeException e2) {
                LOG.debug("Unexpected runtime exception: " + e2, e2);
            }
        }
    }

    protected SocketFactory createSocketFactory() throws IOException {
        return SocketFactory.getDefault();
    }

    protected void initialiseSocket(Socket sock) throws SocketException, IllegalArgumentException {
        try {
            sock.setReceiveBufferSize(socketBufferSize);
            sock.setSendBufferSize(socketBufferSize);
        } catch (SocketException se) {
            LOG.warn("Cannot set socket buffer size = {}", socketBufferSize);
            LOG.debug("Cannot set socket buffer size. Reason: {}. This exception is ignored.", se.getMessage(), se);
        }

        sock.setSoTimeout(soTimeout);

        if (keepAlive != null) {
            sock.setKeepAlive(keepAlive.booleanValue());
        }

        if (soLinger > -1) {
            sock.setSoLinger(true, soLinger);
        } else if (soLinger == -1) {
            sock.setSoLinger(false, 0);
        }

        if (tcpNoDelay != null) {
            sock.setTcpNoDelay(tcpNoDelay.booleanValue());
        }
    }

    protected void initializeStreams() throws IOException {
        try {
            TcpBufferedInputStream buffIn = new TcpBufferedInputStream(socket.getInputStream(), ioBufferSize);
            this.dataIn = new DataInputStream(buffIn);
            TcpBufferedOutputStream outputStream = new TcpBufferedOutputStream(socket.getOutputStream(), ioBufferSize);
            this.dataOut = new DataOutputStream(outputStream);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

    protected String resolveHostName(String host) throws UnknownHostException {
        if (isUseLocalHost()) {
            String localName = InetAddressUtil.getLocalHostName();
            if (localName != null && localName.equals(host)) {
                return "localhost";
            }
        }
        return host;
    }

    private void checkConnected() throws IOException {
        if (!connected.get()) {
            throw new IOException("Cannot send to a non-connected transport.");
        }
    }
}
