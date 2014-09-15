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

public abstract class AmqpResource
{
    private AmqpResourceRequest<?> _openRequest;
    private AmqpResourceRequest<?> _closeRequest;

    public void open(AmqpResourceRequest<?> request)
    {
        _openRequest = request;
        doOpen();
    }

    public void opened()
    {
        if(_openRequest != null)
        {
            _openRequest.onSuccess(null);
            _openRequest = null;
        }
    }

    public void close(AmqpResourceRequest<?> request)
    {
        _closeRequest = request;
        doClose();
    }

    public void closed()
    {
        if(_closeRequest != null)
        {
            _closeRequest.onSuccess(null);
            _closeRequest = null;
        }
    }

    protected abstract void doOpen();

    protected abstract void doClose();
}
