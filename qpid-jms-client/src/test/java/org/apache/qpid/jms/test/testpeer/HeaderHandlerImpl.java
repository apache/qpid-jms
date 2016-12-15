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
    private AmqpPeerRunnable _onCompletion;

    HeaderHandlerImpl(byte[] expectedHeader, byte[] response)
    {
       this(expectedHeader, response, null);
    }

    public HeaderHandlerImpl(byte[] header, byte[] response, AmqpPeerRunnable onCompletion)
    {
        _expectedHeader = header;
        _response = response;
        _onCompletion = onCompletion;
    }

    @Override
    public void header(byte[] header, TestAmqpPeer peer) throws AssertionError
    {
        LOGGER.debug("About to check received header {}", new Binary(header));

        try
        {
            assertThat("Header should match", header, equalTo(_expectedHeader));
        }
        catch(AssertionError ae)
        {
            LOGGER.error("Failure when verifying header", ae);
            peer.assertionFailed(ae);
        }

        if (_response == null || _response.length == 0)
        {
            LOGGER.debug("Skipping header response as none was instructed");
        }
        else
        {
            LOGGER.debug("Sending header response: " + new Binary(_response));
            peer.sendHeader(_response);
        }

        if(_onCompletion != null)
        {
            _onCompletion.run();
        }
        else
        {
            LOGGER.debug("No onCompletion action, doing nothing.");
        }
    }

    @Override
    public String toString()
    {
        return "HeaderHandlerImpl [_expectedHeader=" + new Binary(_expectedHeader) + "]";
    }

    @Override
    public AmqpPeerRunnable getOnCompletionAction()
    {
        return _onCompletion;
    }

    @Override
    public Handler onCompletion(AmqpPeerRunnable onCompletion)
    {
        _onCompletion = onCompletion;
        return this;
    }
}