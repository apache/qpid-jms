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
package org.apache.qpid.jms.engine.temp;

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Transport;

/**
 * AbstractEventHandler
 *
 * TODO: find a new home for this.
 */

public class AbstractEventHandler implements EventHandler
{

    public void onInit(Connection connection) {};
    public void onOpen(Connection connection) {};
    public void onRemoteOpen(Connection connection) {};
    public void onClose(Connection connection) {};
    public void onRemoteClose(Connection connection) {};
    public void onFinal(Connection connection) {};

    public void onInit(Session session) {};
    public void onOpen(Session session) {};
    public void onRemoteOpen(Session session) {};
    public void onClose(Session session) {};
    public void onRemoteClose(Session session) {};
    public void onFinal(Session session) {};

    public void onInit(Link link) {};
    public void onOpen(Link link) {};
    public void onRemoteOpen(Link link) {};
    public void onClose(Link link) {};
    public void onRemoteClose(Link link) {};
    public void onFlow(Link link) {};
    public void onFinal(Link link) {};

    public void onDelivery(Delivery delivery) {};
    public void onTransport(Transport transport) {};

}
