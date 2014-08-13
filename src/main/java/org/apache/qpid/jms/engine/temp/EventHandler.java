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
 * EventHandler
 *
 * TODO: find a new home for this.
 */

public interface EventHandler
{

    void onInit(Connection connection);
    void onOpen(Connection connection);
    void onRemoteOpen(Connection connection);
    void onClose(Connection connection);
    void onRemoteClose(Connection connection);
    void onFinal(Connection connection);

    void onInit(Session session);
    void onOpen(Session session);
    void onRemoteOpen(Session session);
    void onClose(Session session);
    void onRemoteClose(Session session);
    void onFinal(Session session);

    void onInit(Link link);
    void onOpen(Link link);
    void onRemoteOpen(Link link);
    void onClose(Link link);
    void onRemoteClose(Link link);
    void onFlow(Link link);
    void onFinal(Link link);

    void onDelivery(Delivery delivery);
    void onTransport(Transport transport);

}
