/*
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
package org.apache.qpid.jms;

import javax.jms.JMSException;

import org.apache.qpid.jms.meta.JmsAbstractResourceId;
import org.apache.qpid.jms.meta.JmsResource;
import org.apache.qpid.jms.meta.JmsResourceId;
import org.apache.qpid.jms.meta.JmsResourceVistor;

/**
 * Temporary Destination Object
 */
public abstract class JmsTemporaryDestination extends JmsDestination implements JmsResource {

    private boolean deleted;
    private JmsTemporaryDestinationId resourceId;
    private ResourceState state = ResourceState.INITIALIZED;

    public JmsTemporaryDestination() {
        this(null, false);
    }

    public JmsTemporaryDestination(String name, boolean topic) {
        super(name, topic, true);
    }

    @Override
    public JmsResourceId getId() {
        if (resourceId == null) {
            resourceId = new JmsTemporaryDestinationId();
        }

        return resourceId;
    }

    @Override
	public ResourceState getState() {
        return state;
    }

    @Override
	public void setState(ResourceState state) {
        this.state = state;
    }

    void setConnection(JmsConnection connection) {
        this.connection = connection;
    }

    JmsConnection getConnection() {
        return this.connection;
    }

    /**
     * Attempts to delete the destination if there is an assigned Connection object.
     *
     * @throws JMSException if an error occurs or the provider doesn't support
     *         delete of destinations from the client.
     */
    protected void tryDelete() throws JMSException {
        if (connection != null) {
            connection.deleteTemporaryDestination(this);
        }

        deleted = true;
    }

    protected boolean isDeleted() throws JMSException {
        boolean result = deleted;

        if (!result && connection != null) {
            result = connection.isTemporaryDestinationDeleted(this);
        }

        return result;
    }

    @Override
    public void visit(JmsResourceVistor visitor) throws Exception {
        visitor.processDestination(this);
    }

    private class JmsTemporaryDestinationId extends JmsAbstractResourceId implements Comparable<JmsTemporaryDestinationId> {

        public String getDestinationName() {
            return getName();
        }

        @Override
        public int compareTo(JmsTemporaryDestinationId otherId) {
            return getDestinationName().compareTo(otherId.getDestinationName());
        }
    }
}
