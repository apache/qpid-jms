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

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.engine.Delivery;

public class AmqpSentMessageToken
{
    private Delivery _delivery;
    private AmqpSender _amqpSender;
    private AmqpConnection _amqpConnection;
    private AmqpResourceRequest<?> _request;

    public AmqpSentMessageToken(Delivery delivery, AmqpSender sender, AmqpResourceRequest<?> request)
    {
        _delivery = delivery;
        _amqpSender = sender;
        _amqpConnection = _amqpSender.getAmqpConnection();
        _request = request;
    }

    public void settle()
    {
        synchronized (_amqpConnection)
        {
            _delivery.settle();
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("AmqpSentMessage [_delivery=").append(_delivery)
            .append("]");
        return builder.toString();
    }

    void processDeliveryUpdate()
    {
        DeliveryState remoteDeliveryState = _delivery.getRemoteState();
        if(Accepted.getInstance().equals(remoteDeliveryState))
        {
            if(_request != null)
            {
                _request.onSuccess(null);
                _request = null;
            }
        }
        //TODO: check for transactional state acceptance

        //TODO: exception if it isn't accepted.
    }

}
