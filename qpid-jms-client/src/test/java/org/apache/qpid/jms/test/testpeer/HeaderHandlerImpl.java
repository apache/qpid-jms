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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.qpid.proton.amqp.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HeaderHandlerImpl implements HeaderHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderHandlerImpl.class.getName());

    private final byte[] _expectedHeader;
    private final byte[] _response;
    private final Runnable _onSuccess;
    private boolean _isComplete;

    HeaderHandlerImpl(byte[] expectedHeader, byte[] response)
    {
       this(expectedHeader, response, null);
    }

    public HeaderHandlerImpl(byte[] header, byte[] response, Runnable onSuccess)
    {
        _expectedHeader = header;
        _response = response;
        _onSuccess = onSuccess;
    }

    @Override
    public boolean isComplete()
    {
        return _isComplete;
    }

    @Override
    public void header(byte[] header, TestAmqpPeer peer)
    {
        LOGGER.debug("About to check received header {}", new Binary(header));

        assertThat("Header should match", header, equalTo(_expectedHeader));
        peer.sendHeader(_response);
        if(_onSuccess !=null)
        {
            _onSuccess.run();
        }
        _isComplete = true;
    }

    @Override
    public String toString()
    {
        return "HeaderHandlerImpl [_expectedHeader=" + new Binary(_expectedHeader) + "]";
    }
}