/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.jms.test.testpeer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import org.apache.qpid.jms.test.testpeer.basictypes.AmqpError;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestAmqpPeerRunner implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAmqpPeerRunner.class);

    private static final int PORT = 25672;
    private static final int TRACE_FRAME_PAYLOAD_LENGTH = Integer.getInteger("testPeerSendingTraceFramePayloadLength", 1024);

    private final ServerSocket _serverSocket;
    private final boolean useFixedPort = Boolean.getBoolean("testPeerUsesFixedPort");

    private Socket _clientSocket;
    private OutputStream _networkOutputStream;

    private final Object _inputHandlingLock = new Object();
    private final TestAmqpPeer _peer;
    private final TestFrameParser _testFrameParser;
    private volatile boolean _suppressReadExceptionOnClose;
    private volatile boolean _exitReadLoopEarly;
    private volatile boolean _sendSaslHeaderPreEmptively;

    private volatile Throwable _throwable;

    private boolean needClientCert;

    public TestAmqpPeerRunner(TestAmqpPeer peer, SSLContext sslContext, boolean needClientCert) throws IOException
    {
        int port = useFixedPort ? PORT : 0;
        this.needClientCert = needClientCert;

        if (sslContext == null)
        {
            _serverSocket = new ServerSocket(port);
        }
        else
        {
            SSLServerSocketFactory socketFactory = sslContext.getServerSocketFactory();
            _serverSocket = socketFactory.createServerSocket(port);

            SSLServerSocket sslServerSocket = (SSLServerSocket) _serverSocket;
            if (this.needClientCert)
            {
                sslServerSocket.setNeedClientAuth(true);
            }
        }

        _testFrameParser = new TestFrameParser(peer);
        _peer = peer;
    }

    @Override
    public void run()
    {
        boolean attemptingRead = false;
        try
        (
            Socket clientSocket = _serverSocket.accept();
            InputStream networkInputStream = clientSocket.getInputStream();
            OutputStream networkOutputStream = clientSocket.getOutputStream();
        )
        {
            _clientSocket = clientSocket;
            _clientSocket.setTcpNoDelay(true);
            _networkOutputStream = networkOutputStream;

            if (_sendSaslHeaderPreEmptively) {
                byte[] bytes = AmqpHeader.SASL_HEADER;
                LOGGER.debug("Sending header pre-emptively: {}", new Binary(bytes));
                _networkOutputStream.write(bytes);
            }

            int bytesRead;
            byte[] networkInputBytes = new byte[1024];

            LOGGER.trace("Attempting read");
            attemptingRead = true;
            while((bytesRead = networkInputStream.read(networkInputBytes)) != -1)
            {
                attemptingRead = false;
                //prevent stop() from killing the socket while the frame parser might be using it handling input
                synchronized(_inputHandlingLock)
                {
                    ByteBuffer networkInputByteBuffer = ByteBuffer.wrap(networkInputBytes, 0, bytesRead);

                    LOGGER.debug("Read: {} ({} bytes)", new Binary(networkInputBytes, 0, bytesRead), bytesRead);

                    try {
                        _testFrameParser.input(networkInputByteBuffer);
                    } catch (Exception ex) {
                        try {
                            _peer.sendConnectionCloseImmediately(AmqpError.INTERNAL_ERROR, "Problem in peer: " + ex);
                        } catch (Exception ex2) {
                            LOGGER.debug("Exception while sending close frame during handling of previous exception", ex2);
                        }

                        throw ex;
                    }
                }

                if(_exitReadLoopEarly)
                {
                    LOGGER.trace("Exiting read loop early");
                    break;
                }

                LOGGER.trace("Attempting read");
                attemptingRead = true;
            }

            LOGGER.trace("Exited read loop");
        }
        catch (Throwable t)
        {
            if (attemptingRead && _suppressReadExceptionOnClose && t instanceof IOException)
            {
                LOGGER.debug("Caught exception during read, suppressing as expected: " + t, t);
            }
            else if(!_serverSocket.isClosed())
            {
                LOGGER.error("Problem in peer", t);
                _throwable = t;
            }
            else
            {
                if(t instanceof IOException)
                {
                    LOGGER.debug("Caught throwable, ignoring as socket is closed: " + t, t);
                }
                else
                {
                    LOGGER.debug("Caught throwable after socket is closed: " + t);
                    _throwable = t;
                }
            }
        }
        finally
        {
            try
            {
                _serverSocket.close();
            }
            catch (IOException e)
            {
                LOGGER.error("Unable to close server socket", e);
            }
        }
    }

    public void stop() throws IOException
    {
        //wait for the frame parser to handle any input it already had
        synchronized(_inputHandlingLock)
        {
            try
            {
                _serverSocket.close();
            }
            finally
            {
                if(_clientSocket != null)
                {
                    _clientSocket.close();
                }
            }
        }
    }

    public void expectHeader()
    {
        _testFrameParser.expectHeader();
    }

    public void sendBytes(byte[] bytes)
    {
        if(bytes.length > TRACE_FRAME_PAYLOAD_LENGTH) {
            Binary print = new Binary(bytes, 0, TRACE_FRAME_PAYLOAD_LENGTH);
            LOGGER.debug("Sending: {}...(truncated) ({} bytes)", print, bytes.length);
        } else {
            LOGGER.debug("Sending: {} ({} bytes)", new Binary(bytes), bytes.length);
        }

        try
        {
            _networkOutputStream.write(bytes);
            _networkOutputStream.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Throwable getException()
    {
        return _throwable;
    }

    public int getServerPort()
    {
        if (_serverSocket != null)
        {
            return _serverSocket.getLocalPort();
        }

        return -1;
    }

    public Socket getClientSocket()
    {
        return _clientSocket;
    }

    public void setSuppressReadExceptionOnClose(boolean suppress)
    {
        _suppressReadExceptionOnClose = suppress;
    }

    public boolean isNeedClientCert() {
        return needClientCert;
    }

    public void exitReadLoopEarly() {
        _exitReadLoopEarly = true;
    }

    public void setSendSaslHeaderPreEmptively(boolean sendSaslHeaderPreEmptively) {
        _sendSaslHeaderPreEmptively = sendSaslHeaderPreEmptively;
    }

    public boolean isSendSaslHeaderPreEmptively() {
        return _sendSaslHeaderPreEmptively;
    }

    public boolean isSSL() {
        return _serverSocket instanceof SSLServerSocket;
    }
}
