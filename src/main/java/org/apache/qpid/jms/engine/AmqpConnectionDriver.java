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

public class AmqpConnectionDriver
{
    private static Logger _logger = Logger.getLogger("qpid.jms-client.connection.driver");
    private static final ProtonFactoryLoader<DriverFactory> driverFactoryLoader = new ProtonFactoryLoader<DriverFactory>(DriverFactory.class);
    private DriverFactory _driverFactory;
    private Driver _driver;

    private final ConcurrentHashMap<AmqpConnection,Boolean> _updated = new ConcurrentHashMap<AmqpConnection,Boolean>();

    public AmqpConnectionDriver() throws IOException
    {
        _driverFactory = defaultDriverFactory();
        _driver = _driverFactory.createDriver();
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

        new Thread(new DriverRunnable(amqpConnection)).start();
    }

    private static DriverFactory defaultDriverFactory()
    {
        return driverFactoryLoader.loadFactory();
    }

    public class DriverRunnable implements Runnable
    {
        //TODO: delete
        private AmqpConnection _connection;

        public DriverRunnable(AmqpConnection connection)
        {
            _connection = connection;
        }

        @Override
        public void run()
        {
            while(true)
            {
                Connector<?> connector;
                try
                {
                    for (Connector<?> c : _driver.connectors())
                    {
                        AmqpConnection amqpConnection = (AmqpConnection) (c.getContext());
                        if(isUpdated(amqpConnection))
                        {                            
                            processConnector(c);
                        }
                    }

                    while((connector = _driver.connector()) != null)
                    {
                        processConnector(connector);
                    }
                    _logger.log(Level.FINEST, "Waiting");
                    _driver.doWait(AmqpConnection.TIMEOUT);
                    _logger.log(Level.FINEST, "Stopped Waiting");
                }
                catch (IOException e)
                {
                    // TODO
                    e.printStackTrace();
                    break;
                }
            }
        }

        private boolean isUpdated(AmqpConnection amqpConnection)
        {
            return _updated.remove(amqpConnection) != null;
        }

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
                }

                amqpConnection.notifyAll();
            }
        }
    }

    public void wakeup()
    {
        _driver.wakeup();
    }

    public void updated(AmqpConnection amqpConnection)
    {
        _updated.put(amqpConnection, Boolean.TRUE);
    }
}