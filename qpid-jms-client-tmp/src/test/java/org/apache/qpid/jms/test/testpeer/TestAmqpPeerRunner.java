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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.qpid.proton.amqp.Binary;

class TestAmqpPeerRunner implements Runnable
{
    private static final Logger _logger = Logger.getLogger(TestAmqpPeerRunner.class.getName());

    private final ServerSocket _serverSocket;

    /** TODO handle multiple connections */
    private Socket _clientSocket;
    private OutputStream _networkOutputStream;

    private final Object _inputHandlingLock = new Object();
    private final TestFrameParser _testFrameParser;

    private volatile Throwable _throwable;

    public TestAmqpPeerRunner(int port, TestAmqpPeer peer) throws IOException
    {
        _serverSocket = new ServerSocket(port);
        _testFrameParser = new TestFrameParser(peer);
    }

    @Override
    public void run()
    {
        try
        (
            Socket clientSocket = _serverSocket.accept();
            InputStream networkInputStream = clientSocket.getInputStream();
            OutputStream networkOutputStream = clientSocket.getOutputStream();
        )
        {
            _clientSocket = clientSocket;
            _networkOutputStream = networkOutputStream;

            int bytesRead;
            byte[] networkInputBytes = new byte[1024];

            _logger.finest("Attempting read");
            while((bytesRead = networkInputStream.read(networkInputBytes)) != -1)
            {
                //prevent stop() from killing the socket while the frame parser might be using it handling input
                synchronized(_inputHandlingLock)
                {
                    ByteBuffer networkInputByteBuffer = ByteBuffer.wrap(networkInputBytes, 0, bytesRead);

                    _logger.finer("Read: " + new Binary(networkInputBytes, 0, bytesRead));

                    _testFrameParser.input(networkInputByteBuffer);
                }
                _logger.finest("Attempting read");
            }

            _logger.finest("Exited read loop");
        }
        catch (Throwable t)
        {
            if(!_serverSocket.isClosed())
            {
                _logger.log(Level.SEVERE, "Problem in peer", t);
                _throwable = t;
            }
            else
            {
                _logger.fine("Caught throwable, ignoring as socket is closed: " + t);
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
                _logger.log(Level.SEVERE, "Unable to close server socket", e);
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
        _logger.finer("Sending: " + new Binary(bytes));
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
}
