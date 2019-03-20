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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.jndi.JNDIStorable;

/**
 * Jms Destination
 */
public abstract class JmsDestination extends JNDIStorable implements Externalizable, javax.jms.Destination, Comparable<JmsDestination> {
    private static final String NAME_PROP = "address";
    private static final String LEGACY_NAME_PROP = "name";

    protected transient String address;
    protected transient boolean topic;
    protected transient boolean temporary;
    protected transient boolean durable;
    protected transient int hashValue;
    protected transient JmsConnection connection;

    protected JmsDestination(String address, boolean topic, boolean temporary, boolean durable) {
        this.address = address;
        this.topic = topic;
        this.temporary = temporary;
        this.durable = durable;
    }

    @Override
    public String toString() {
        return address;
    }

    /**
     * @return address of destination
     */
    public String getAddress() {
        return this.address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * @return the topic
     */
    public boolean isTopic() {
        return this.topic;
    }

    /**
     * @return the temporary
     */
    public boolean isTemporary() {
        return this.temporary;
    }

    /**
     * @return the durable
     */
    public boolean isDurable() {
        return this.durable;
    }

    /**
     * @return true if a Topic
     */
    public boolean isQueue() {
        return !this.topic;
    }

    @Override
    protected Map<String, String> buildFromProperties(Map<String, String> props) {
        setAddress(getProperty(props, NAME_PROP, getProperty(props, LEGACY_NAME_PROP, "")));

        Map<String, String> unused = new HashMap<String,String>(props);
        unused.remove(NAME_PROP);
        unused.remove(LEGACY_NAME_PROP);

        return Collections.unmodifiableMap(unused);
    }

    @Override
    protected void populateProperties(Map<String, String> props) {
        props.put(NAME_PROP, getAddress());
    }

    /**
     * @param other
     *        the Object to be compared.
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object.
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(JmsDestination other) {
        if (other != null) {
            if (this == other) {
                return 0;
            }
            if (isTemporary() == other.isTemporary()) {
                return getAddress().compareTo(other.getAddress());
            }
            return -1;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JmsDestination other = (JmsDestination) o;
        if (address == null && other.address != null) {
            return false;
        } else if (address != null && !address.equals(other.address)) {
            return false;
        }

        if (temporary != other.temporary) {
            return false;
        }
        if (topic != other.topic) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        if (hashValue == 0) {
            final int prime = 31;
            hashValue = 1;
            hashValue = prime * hashValue + ((address == null) ? 0 : address.hashCode());
            hashValue = prime * hashValue + (temporary ? 1231 : 1237);
            hashValue = prime * hashValue + (topic ? 1231 : 1237);
        }
        return hashValue;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(getAddress());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setAddress(in.readUTF());
    }
}
