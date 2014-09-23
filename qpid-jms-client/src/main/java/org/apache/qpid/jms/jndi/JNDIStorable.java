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
package org.apache.qpid.jms.jndi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;

/**
 * Facilitates objects to be stored in JNDI as properties
 *
 * @since 1.0
 */
public abstract class JNDIStorable implements Referenceable, Externalizable {

    private Map<String, String> properties;

    /**
     * Set the properties that will represent the instance in JNDI
     *
     * @param props
     */
    protected abstract void buildFromProperties(Map<String, String> props);

    /**
     * Initialize the instance from properties stored in JNDI
     *
     * @param props
     */
    protected abstract void populateProperties(Map<String, String> props);

    /**
     * set the properties for this instance as retrieved from JNDI
     *
     * @param props
     */
    public synchronized void setProperties(Map<String, String> props) {
        this.properties = props;
        buildFromProperties(props);
    }

    /**
     * Get the properties from this instance for storing in JNDI
     *
     * @return the properties
     */
    public synchronized Map<String, String> getProperties() {
        if (this.properties == null) {
            this.properties = new HashMap<String, String>();
        }
        populateProperties(this.properties);
        return this.properties;
    }

    /**
     * Retrieve a Reference for this instance to store in JNDI
     *
     * @return the built Reference
     * @throws NamingException
     *         if error on building Reference
     */
    @Override
    public Reference getReference() throws NamingException {
        return JNDIReferenceFactory.createReference(this.getClass().getName(), this);
    }

    /**
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Map<String, String> props = (Map<String, String>) in.readObject();
        if (props != null) {
            setProperties(props);
        }
    }

    /**
     * @param out
     * @throws IOException
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getProperties());
    }

    protected String getProperty(Map<String, String> map, String key, String defaultValue) {
        String value = map.get(key);
        if (value != null) {
            return value;
        }
        return defaultValue;
    }
}
