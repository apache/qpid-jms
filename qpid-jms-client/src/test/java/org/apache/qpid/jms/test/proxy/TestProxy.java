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
package org.apache.qpid.jms.test.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProxy implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TestProxy.class);
    private static final int TIMEOUT_IN_S = 2;
    private AsynchronousServerSocketChannel serverSocketChannel;
    private AtomicInteger connectCount = new AtomicInteger();
    private ProxyReadHandler readHandler = new ProxyReadHandler();
    private ProxyWriteHandler writeHandler = new ProxyWriteHandler();
    private int port;
    private final ProxyType type;

    public enum ProxyType {
        SOCKS5, HTTP
    }

    public TestProxy(ProxyType type) throws IOException {
        Objects.requireNonNull(type, "Proxy type must be given");

        this.type = type;
    }

    public int getPort() {
        return port;
    }

    public int getSuccessCount() {
        return connectCount.get();
    }

    public void start() throws IOException {
        serverSocketChannel = AsynchronousServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(0));
        port = ((InetSocketAddress) serverSocketChannel.getLocalAddress()).getPort();
        LOG.info("Bound listen socket to port {}, waiting for clients...", port);
        serverSocketChannel.accept(null, new ServerConnectionHandler());
    }

    @Override
    public void close() {
        LOG.info("stopping proxy server");
        // Close Server Socket
        if(serverSocketChannel != null) {
            try {
                LOG.info("Terminating server socket");
                serverSocketChannel.close();
            } catch (Exception e) {
                LOG.error("Cannot close server socket ", e);
            }
        }
    }

    private boolean processHandshakeMessages(ProxyConnectionState attachment) {
        if (attachment.handshakePhase == HandshakePhase.INITIAL) {
            byte first = attachment.buffer.get(0);
            if (first == 0x5) {
                attachment.handshakePhase = HandshakePhase.SOCKS5_1;
            } else {
                attachment.handshakePhase = HandshakePhase.HTTP;
            }
        }

        if(!assertExpectedHandshakeType(attachment.handshakePhase)) {
            LOG.error("Unexpected handshake phase '" + attachment.handshakePhase + "' for proxy of type: " + type);
            return false;
        }

        switch (attachment.handshakePhase) {
            case SOCKS5_1:
                return processSocks5Handshake1(attachment);
            case SOCKS5_2:
                return processSocks5Handshake2(attachment);
            case HTTP:
                return processHttpHandshake(attachment);
            default:
                LOG.error("wrong handshake phase");
                return false;
        }
    }

    private boolean assertExpectedHandshakeType(HandshakePhase handshakePhase) {
        switch (handshakePhase) {
            case SOCKS5_1:
            case SOCKS5_2:
                return type == ProxyType.SOCKS5;
            case HTTP:
                return type == ProxyType.HTTP;
            default:
                LOG.error("Unknown handshake phase type:" + handshakePhase);
                return false;
        }
    }

    private boolean processHttpHandshake(ProxyConnectionState attachment) {
        String requestString = StandardCharsets.ISO_8859_1.decode(attachment.buffer).toString();
        LOG.debug("Request received: {}", requestString);
        String requestType = requestString.substring(0, requestString.indexOf(' '));
        String hostandport = requestString.substring(requestType.length() + 1);
        hostandport = hostandport.substring(0, hostandport.indexOf(' '));
        String hostname = hostandport.substring(0, hostandport.indexOf(":"));
        int port = Integer.parseInt(hostandport.substring(hostname.length() + 1));

        String line;
        if (requestType.equals("CONNECT")) {
            LOG.info("CONNECT to {}:{}", hostname, port);
            if (connectToServer(hostname, port, attachment)) {
                attachment.handshakePhase = HandshakePhase.CONNECTED;
                line = "HTTP/1.1 200 Connection established\r\n\r\n";
            } else {
                line = "HTTP/1.1 504 Gateway Timeout\r\n\r\n";
            }
        } else {
            LOG.error("unsupported request type {}", requestType);
            line = "HTTP/1.1 502 Bad Gateway\r\n\r\n";
        }
        attachment.buffer.clear();
        attachment.buffer.put(StandardCharsets.ISO_8859_1.encode(line));
        attachment.buffer.flip();
        return true;
    }

    private boolean processSocks5Handshake1(ProxyConnectionState attachment) {
        byte version = attachment.buffer.get();
        if (version != 0x5) {
            LOG.error("SOCKS Version {} not supported", version);
            closeChannel(attachment.readChannel);
            return false;
        }
        attachment.buffer.clear();
        attachment.buffer.put(version);
        attachment.buffer.put((byte) 0x0); // no authentication required
        attachment.buffer.flip();
        LOG.info("SOCKS5 connection initialized, no authentication required");
        attachment.handshakePhase = HandshakePhase.SOCKS5_2;
        return true;
    }

    private boolean processSocks5Handshake2(ProxyConnectionState attachment) {
        byte version = attachment.buffer.get();
        if (version != 0x5) {
            LOG.error("SOCKS Version {} not supported", version);
            closeChannel(attachment.readChannel);
            return false;
        }
        byte command = attachment.buffer.get();
        if (command != 0x1) {
            LOG.error("CMD {} not supported", command);
            closeChannel(attachment.readChannel);
            return false;
        }
        attachment.buffer.get(); // skip the next byte (should be 0x0)
        byte addressType = attachment.buffer.get();
        if (addressType != 0x3) {
            LOG.error("Address Type {} not supported", addressType);
            closeChannel(attachment.readChannel);
            return false;
        }
        int size = attachment.buffer.get() & 0xFF; // unsigned byte
        byte[] hostBytes = new byte[size];
        attachment.buffer.get(hostBytes);
        String hostname = new String(hostBytes, StandardCharsets.UTF_8);
        int port = attachment.buffer.getShort() & 0xffff; // unsigned short
        // now we have hostname and port connect to the actual listen
        LOG.info("Create SOCKS5 connection to {}:{}", hostname, port);
        if (!connectToServer(hostname, port, attachment)) {
            return false;
        }

        // write back the client response
        attachment.buffer.rewind();
        attachment.buffer.put(1, (byte) 0x0);
        attachment.handshakePhase = HandshakePhase.CONNECTED; // handshake done
        return true;
    }

    private boolean isInHandshake(ProxyConnectionState attachment) {
        return attachment.handshakePhase != HandshakePhase.CONNECTED;
    }

    private boolean connectToServer(String hostname, int port, ProxyConnectionState attachment) {
        try {
            AsynchronousSocketChannel serverChannel = AsynchronousSocketChannel.open();
            try {
                serverChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            } catch (IOException e) {
                LOG.error("Failed to set TCP_NODELAY before connect, closing channel", e);
                closeChannel(serverChannel);
                return false;
            }

            SocketAddress serverAddr = new InetSocketAddress(hostname, port);
            Future<Void> connectResult = serverChannel.connect(serverAddr);
            connectResult.get(TIMEOUT_IN_S, TimeUnit.SECONDS);
            attachment.writeChannel = serverChannel;
            int connectionNumber = connectCount.incrementAndGet();
            LOG.info("Connection {} to {}:{} established", connectionNumber, hostname, port);
            ProxyConnectionState serverState = new ProxyConnectionState();
            serverState.readChannel = attachment.writeChannel;
            serverState.writeChannel = attachment.readChannel;
            serverState.buffer = ByteBuffer.allocate(4096);
            serverState.handshakePhase = HandshakePhase.CONNECTED;

            // read from server
            serverState.readChannel.read(serverState.buffer, 2, TimeUnit.SECONDS, serverState, readHandler);
            return true;
        } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("connection failed ", e);
            closeChannel(attachment.readChannel);
            return false;
        }
    }

    private void closeChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                LOG.error("cannot close", e);
            }
        }
    }

    private static class ProxyConnectionState {
        AsynchronousSocketChannel readChannel;
        AsynchronousSocketChannel writeChannel;
        ByteBuffer buffer;
        HandshakePhase handshakePhase;
    }

    private enum HandshakePhase {
        INITIAL, HTTP, SOCKS5_1, SOCKS5_2, CONNECTED
    }

    private class ServerConnectionHandler implements CompletionHandler<AsynchronousSocketChannel, Object> {

        @Override
        public void completed(AsynchronousSocketChannel clientChannel, Object attachment) {
            // keep listening
            serverSocketChannel.accept(attachment, this);

            ProxyConnectionState clientState = new ProxyConnectionState();
            clientState.readChannel = clientChannel;
            clientState.buffer = ByteBuffer.allocate(4096);
            clientState.handshakePhase = HandshakePhase.INITIAL;

            try {
                clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            } catch (IOException e) {
                LOG.error("Failed to set TCP_NODELAY after accept, closing channel", e);
                closeChannel(clientChannel);
                return;
            }

            // read from readChannel
            clientChannel.read(clientState.buffer, TIMEOUT_IN_S, TimeUnit.SECONDS, clientState, readHandler);
        }

        @Override
        public void failed(Throwable e, Object attachment) {
            if (!(e instanceof ClosedChannelException)) {
                LOG.error("failed to accept connection ", e);
            }
            closeChannel(serverSocketChannel);
        }
    }

    private class ProxyReadHandler implements CompletionHandler<Integer, ProxyConnectionState> {

        @Override
        public void completed(Integer result, ProxyConnectionState attachment) {
            // connection closed
            if (result == -1) {
                LOG.info("read connection closed ({})", attachment.readChannel);
                closeChannel(attachment.readChannel);
                return;
            }
            LOG.info("read {} bytes (from {})", result, attachment.readChannel);

            attachment.buffer.flip();

            if (isInHandshake(attachment)) {
                if (processHandshakeMessages(attachment)) {
                    attachment.readChannel.write(attachment.buffer, TIMEOUT_IN_S, TimeUnit.SECONDS, attachment, writeHandler);
                }
            } else {
                // handshake done
                if (attachment.writeChannel == null) {
                    LOG.error("Invalid");
                    closeChannel(attachment.readChannel);
                    return;
                }
                // forward data to writeChannel
                if (attachment.writeChannel.isOpen()) {
                    attachment.writeChannel.write(attachment.buffer, TIMEOUT_IN_S, TimeUnit.SECONDS, attachment, writeHandler);
                } else {
                    closeChannel(attachment.readChannel);
                }
            }
        }

        @Override
        public void failed(Throwable e, ProxyConnectionState attachment) {
            if (!(e instanceof ClosedChannelException)) {
                LOG.info("read failed", e);
            }
            closeChannel(attachment.writeChannel);
            closeChannel(attachment.readChannel);
        }
    }

    private class ProxyWriteHandler implements CompletionHandler<Integer, ProxyConnectionState> {

        @Override
        public void completed(Integer result, ProxyConnectionState attachment) {
            // connection closed
            if (result == -1) {
                LOG.info("write connection closed");
                closeChannel(attachment.writeChannel);
                closeChannel(attachment.readChannel);
                return;
            }
            LOG.debug("wrote {} bytes", result);
            attachment.buffer.clear();
            if (attachment.readChannel.isOpen()) {
                attachment.readChannel.read(attachment.buffer, attachment, readHandler);
            } else {
                closeChannel(attachment.writeChannel);
            }
        }

        @Override
        public void failed(Throwable e, ProxyConnectionState attachment) {
            if (!(e instanceof ClosedChannelException)) {
                LOG.info("write failed", e);
            }
            closeChannel(attachment.writeChannel);
            closeChannel(attachment.readChannel);
        }
    }
}
