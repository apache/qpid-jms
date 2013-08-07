/*
 *
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
 *
 */
package org.apache.qpid.jms.engine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.qpid.proton.ProtonFactoryLoader;
import org.apache.qpid.proton.driver.Connector;
import org.apache.qpid.proton.driver.Driver;
import org.apache.qpid.proton.driver.DriverFactory;
import org.apache.qpid.proton.engine.Sasl;

/**
 * <p>
 * Asynchronously processes local and remote updates to the AmqpXXX objects on
 * the registered {@link AmqpConnection}s. Specifically:</p>
 * <p>
 * - Once notified that a connection has local updates via {@link #setLocallyUpdated(AmqpConnection)},
 * these updates are written to the network.
 * </p>
 * <p>
 * - Reads any remote updates from the network and updates the local AmqpXXX objects accordingly.
 * </p>
 * <p>
 * Thread-safe.
 * </p>
 *
 * TODO verify that multiple connections are handled properly
 */
public class AmqpConnectionDriver
{
    private static Logger _logger = Logger.getLogger(AmqpConnectionDriver.class.getName());

    private final Driver _driver;

    private final ConcurrentHashMap<AmqpConnection,Boolean> _locallyUpdatedConnections =
            new ConcurrentHashMap<AmqpConnection,Boolean>();

    private DriverRunnable _driverRunnable;
    private Thread _driverThread;

    public enum AmqpDriverState
    {
        UNINIT,
        OPEN,
        STOPPED,
        ERROR;
    }

    public AmqpConnectionDriver() throws IOException
    {
        DriverFactory driverFactory = new ProtonFactoryLoader<DriverFactory>(DriverFactory.class).loadFactory();
        _driver = driverFactory.createDriver();
    }

    public void registerConnection(AmqpConnection amqpConnection)
    {
        String remoteHost = amqpConnection.getRemoteHost();
        int port = amqpConnection.getPort();

        SocketChannel channel = null;
        try
        {
            channel = SocketChannel.open();
            channel.configureBlocking(true);
            channel.connect(new InetSocketAddress(remoteHost, port));
            channel.configureBlocking(false);
        }
        catch (IOException e)
        {
            // TODO this will be done elsewhere in future
            throw new RuntimeException(e);
        }

        //TODO: everything below is hacky and probably only works for a single connection.
        Connector<AmqpConnection> connector = _driver.createConnector(channel, amqpConnection);
        connector.setConnection(amqpConnection.getConnection());

        Sasl sasl = connector.sasl();
        if (sasl != null)
        {
            sasl.client();
        }

        amqpConnection.setSasl(sasl);

        _driverRunnable = new DriverRunnable();
        _driverThread = new Thread(_driverRunnable); // TODO set a sensible thread name
        _driverThread.start();
    }

    public void stop() throws InterruptedException
    {
        _driverRunnable.requestStop();
        _driverThread.join(AmqpConnection.TIMEOUT);
        AmqpDriverState state = _driverRunnable.getState();
        if(state != AmqpDriverState.STOPPED)
        {
            throw new IllegalStateException("After trying to stop, Driver is in state " + state);
        }
    }

    private class DriverRunnable implements Runnable
    {
        private volatile AmqpDriverState _state = AmqpDriverState.UNINIT;
        private volatile boolean _stopRequested;

        @Override
        public void run()
        {
            _state = AmqpDriverState.OPEN;
            while(!_stopRequested)
            {
                try
                {
                    // Process connectors with local updates
                    for (Connector<?> c : _driver.connectors())
                    {
                        AmqpConnection amqpConnection = (AmqpConnection) (c.getContext());
                        if(getAndClearLocallyUpdated(amqpConnection))
                        {
                            processConnector(c);
                        }
                    }

                    // Process connectors with in-bound data
                    // (may incidentally also process connectors with out-bound data whose
                    // sockets have just become writeable)
                    Connector<?> connector;
                    while((connector = _driver.connector()) != null)
                    {
                        processConnector(connector);
                    }

                    waitForLocalOrRemoteUpdates();
                }
                catch (IOException e)
                {
                    // TODO proper error handling
                    _logger.log(Level.SEVERE, "Driver error", e);
                    _state = AmqpDriverState.ERROR;
                    break;
                }
            }

            closeAndDestroyDriver();
        }

        private void closeAndDestroyDriver()
        {
            try
            {
                for (Connector<?> c : _driver.connectors())
                {
                    c.close();
                }

                _driver.destroy();

                if(_state != AmqpDriverState.ERROR)
                {
                    _state = AmqpDriverState.STOPPED;
                }
                if(_logger.isLoggable(Level.FINE))
                {
                    _logger.fine(this + " closed and destroyed driver");
                }
            }
            catch(Exception e)
            {
                 // TODO proper error handling
                _logger.log(Level.SEVERE, "Driver error", e);
                _state = AmqpDriverState.ERROR;
            }
        }

        private void waitForLocalOrRemoteUpdates()
        {
            // We're careful below whether we call Driver.doWait().
            // Any prior setLocallyUpdated() calls would have set the Driver's wake-up status,
            // but this may have been cleared by the Driver.connector() call above.
            // Therefore, we guard the doWait() call with a check for pending local updates.

            if(!_stopRequested && _locallyUpdatedConnections.isEmpty())
            {
                _driver.doWait(AmqpConnection.TIMEOUT);
            }
        }

        public void requestStop()
        {
            _stopRequested = true;
            _driver.wakeup();
        }

        public AmqpDriverState getState()
        {
            return _state;
        }

        private boolean getAndClearLocallyUpdated(AmqpConnection amqpConnection)
        {
            return _locallyUpdatedConnections.remove(amqpConnection) != null;
        }

        /**
         * Handle the connector's inbound data and send its outbound data
         */
        public void processConnector(Connector<?> connector) throws IOException
        {
            AmqpConnection amqpConnection = (AmqpConnection) (connector.getContext());
            synchronized(amqpConnection)
            {
                do
                {
                    connector.process();
                }
                while (amqpConnection.process());

                if(amqpConnection.isClosed())
                {
                    connector.destroy();
                    _state = AmqpDriverState.STOPPED;
                }

                amqpConnection.notifyAll();
            }
        }
    }

    /**
     * Indicate that at least one AmqpXXX object on the supplied connection
     * has been locally updated, so the driver knows it needs to send updates
     * to the peer.
     */
    public void setLocallyUpdated(AmqpConnection amqpConnection)
    {
        _locallyUpdatedConnections.put(amqpConnection, Boolean.TRUE);
        _driver.wakeup();
    }
}