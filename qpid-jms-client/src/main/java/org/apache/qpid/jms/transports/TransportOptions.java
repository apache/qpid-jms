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
package org.apache.qpid.jms.transports;

/**
 * Encapsulates all the TCP Transport options in one configuration object.
 */
public class TransportOptions implements Cloneable {

    public static final int DEFAULT_SEND_BUFFER_SIZE = 64 * 1024;
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = DEFAULT_SEND_BUFFER_SIZE;
    public static final int DEFAULT_TRAFFIC_CLASS = 0;
    public static final boolean DEFAULT_TCP_NO_DELAY = true;
    public static final boolean DEFAULT_TCP_KEEP_ALIVE = false;
    public static final int DEFAULT_SO_LINGER = Integer.MIN_VALUE;
    public static final int DEFAULT_SO_TIMEOUT = -1;
    public static final int DEFAULT_CONNECT_TIMEOUT = 60000;
    public static final int DEFAULT_TCP_PORT = 5672;
    public static final boolean DEFAULT_USE_EPOLL = true;
    public static final boolean DEFAULT_USE_KQUEUE = false;
    public static final boolean DEFAULT_TRACE_BYTES = false;

    private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
    private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
    private int trafficClass = DEFAULT_TRAFFIC_CLASS;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int soTimeout = DEFAULT_SO_TIMEOUT;
    private int soLinger = DEFAULT_SO_LINGER;
    private boolean tcpKeepAlive = DEFAULT_TCP_KEEP_ALIVE;
    private boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;
    private int defaultTcpPort = DEFAULT_TCP_PORT;
    private boolean useEpoll = DEFAULT_USE_EPOLL;
    private boolean useKQueue = DEFAULT_USE_KQUEUE;
    private boolean traceBytes = DEFAULT_TRACE_BYTES;

    /**
     * @return the currently set send buffer size in bytes.
     */
    public int getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * Sets the send buffer size in bytes, the value must be greater than zero
     * or an {@link IllegalArgumentException} will be thrown.
     *
     * @param sendBufferSize
     *        the new send buffer size for the TCP Transport.
     *
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public void setSendBufferSize(int sendBufferSize) {
        if (sendBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.sendBufferSize = sendBufferSize;
    }

    /**
     * @return the currently configured receive buffer size in bytes.
     */
    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * Sets the receive buffer size in bytes, the value must be greater than zero
     * or an {@link IllegalArgumentException} will be thrown.
     *
     * @param receiveBufferSize
     *        the new receive buffer size for the TCP Transport.
     *
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public void setReceiveBufferSize(int receiveBufferSize) {
        if (receiveBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.receiveBufferSize = receiveBufferSize;
    }

    /**
     * @return the currently configured traffic class value.
     */
    public int getTrafficClass() {
        return trafficClass;
    }

    /**
     * Sets the traffic class value used by the TCP connection, valid
     * range is between 0 and 255.
     *
     * @param trafficClass
     *        the new traffic class value.
     *
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public void setTrafficClass(int trafficClass) {
        if (trafficClass < 0 || trafficClass > 255) {
            throw new IllegalArgumentException("Traffic class must be in the range [0..255]");
        }

        this.trafficClass = trafficClass;
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

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getSoLinger() {
        return soLinger;
    }

    public void setSoLinger(int soLinger) {
        this.soLinger = soLinger;
    }

    public boolean isTcpKeepAlive() {
        return tcpKeepAlive;
    }

    public void setTcpKeepAlive(boolean keepAlive) {
        this.tcpKeepAlive = keepAlive;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getDefaultTcpPort() {
        return defaultTcpPort;
    }

    public void setDefaultTcpPort(int defaultTcpPort) {
        this.defaultTcpPort = defaultTcpPort;
    }


    /**
     * @return the true if use of of the netty epoll transport is used.
     */
    public boolean isUseEpoll() {
        return useEpoll;
    }

    /**
     * Determines if the netty epoll transport can be used if available on this platform.
     *
     * @param useEpoll
     * 		should use of available epoll transport be used.
     */
    public void setUseEpoll(boolean useEpoll) {
        this.useEpoll = useEpoll;
    }

    /**
     * @return the true if use of of the netty kqueue transport is used.
     */
    public boolean isUseKQueue() {
        return useKQueue;
    }

    /**
     * Determines if the netty kqueue transport can be used if available on this platform.
     *
     * @param useKQueue
     * 		should use of available kqueue transport be used.
     */
    public void setUseKQueue(boolean useKQueue) {
        this.useKQueue = useKQueue;
    }

    /**
     * @return true if the transport should enable byte tracing
     */
    public boolean isTraceBytes() {
        return traceBytes;
    }

    /**
     * Determines if the transport should add a logger for bytes in / out
     *
     * @param traceBytes
     * 		should the transport log the bytes in and out.
     */
    public void setTraceBytes(boolean traceBytes) {
        this.traceBytes = traceBytes;
    }

    @Override
    public TransportOptions clone() {
        return copyOptions(new TransportOptions());
    }

    public boolean isSSL() {
        return false;
    }

    protected TransportOptions copyOptions(TransportOptions copy) {
        copy.setConnectTimeout(getConnectTimeout());
        copy.setReceiveBufferSize(getReceiveBufferSize());
        copy.setSendBufferSize(getSendBufferSize());
        copy.setSoLinger(getSoLinger());
        copy.setSoTimeout(getSoTimeout());
        copy.setTcpKeepAlive(isTcpKeepAlive());
        copy.setTcpNoDelay(isTcpNoDelay());
        copy.setTrafficClass(getTrafficClass());
        copy.setDefaultTcpPort(getDefaultTcpPort());
        copy.setUseEpoll(isUseEpoll());
        copy.setTraceBytes(isTraceBytes());

        return copy;
    }
}
