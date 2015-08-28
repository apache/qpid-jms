/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.provider.amqp;

import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.jms.meta.JmsConsumerInfo;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queue Browser implementation for AMQP
 */
public class AmqpQueueBrowser extends AmqpConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpQueueBrowser.class);

    public AmqpQueueBrowser(AmqpSession session, JmsConsumerInfo info) {
        super(session, info);
    }

    @Override
    public void pull(final long timeout) {

        // Zero prefetch QueueBrowser behaves the same as a standard pull consumer.
        if (resource.getPrefetchSize() == 0) {
            super.pull(timeout);
            return;
        }

        LOG.trace("Pull on browser {} with timeout = {}", getConsumerId(), timeout);

        // Pull for browser is called when there are no available messages buffered.
        // If we still have some to dispatch then no pull is needed otherwise we might
        // need to attempt try and drain to end the browse.
        if (getEndpoint().getQueued() == 0) {
            final ScheduledFuture<?> future = getSession().schedule(new Runnable() {

                @Override
                public void run() {
                    // Try for one last time to pull a message down, if this
                    // fails then we can end the browse otherwise the link credit
                    // will get updated on the next sent disposition and we will
                    // end up back here if no more messages arrive.
                    LOG.trace("Browser {} attemptig to force a message dispatch");
                    getEndpoint().drain(1);
                    pullRequest = new PullRequest();
                    session.getProvider().pumpToProtonTransport();
                }
            }, timeout);

            pullRequest = new BrowseEndPullRequest(future);
        }
    }

    @Override
    protected void configureSource(Source source) {
        if (resource.isBrowser()) {
            source.setDistributionMode(COPY);
        }

        super.configureSource(source);
    }

    @Override
    public boolean isBrowser() {
        return true;
    }

    //----- Inner classes used in message pull operations --------------------//

    protected class BrowseEndPullRequest extends TimedPullRequest {

        public BrowseEndPullRequest(ScheduledFuture<?> completionTask) {
            super(completionTask);
        }

        @Override
        public void onFailure(Throwable result) {
            // Nothing to do, the timer will take care of the end of browse signal.
        }
    }
}
