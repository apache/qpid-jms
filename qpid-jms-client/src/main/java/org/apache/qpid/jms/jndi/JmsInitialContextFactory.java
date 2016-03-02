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
package org.apache.qpid.jms.jndi;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;

/**
 * A factory of the StompJms InitialContext which contains
 * {@link javax.jms.ConnectionFactory} instances as well as a child context
 * called <i>destinations</i> which contain all of the current active
 * destinations, in child context depending on the QoS such as transient or
 * durable and queue or topic.
 *
 * @since 1.0
 */
public class JmsInitialContextFactory implements InitialContextFactory {

    static final String[] DEFAULT_CONNECTION_FACTORY_NAMES = {
        "ConnectionFactory", "QueueConnectionFactory", "TopicConnectionFactory" };

    static final String CONNECTION_FACTORY_KEY_PREFIX = "connectionfactory.";
    static final String QUEUE_KEY_PREFIX = "queue.";
    static final String TOPIC_KEY_PREFIX = "topic.";
    static final String CONNECTION_FACTORY_DEFAULT_KEY_PREFIX = "default." + CONNECTION_FACTORY_KEY_PREFIX;
    static final String CONNECTION_FACTORY_PROPERTY_KEY_PREFIX = "property." + CONNECTION_FACTORY_KEY_PREFIX;

    @SuppressWarnings("unchecked")
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        // Copy the environment to ensure we don't modify/reference it, it belongs to the caller.
        Hashtable<Object, Object> environmentCopy = new Hashtable<Object, Object>();
        environmentCopy.putAll(environment);

        // Check for an *optional* properties file use to augment the environment
        String location = null;
        if (environmentCopy.containsKey(Context.PROVIDER_URL)) {
            location = (String) environment.get(Context.PROVIDER_URL);
        } else {
            location = System.getProperty(Context.PROVIDER_URL);
        }

        try {
            if (location != null) {
                BufferedInputStream inputStream;

                try {
                    URL fileURL = new URL(location);
                    inputStream = new BufferedInputStream(fileURL.openStream());
                } catch (MalformedURLException e) {
                    inputStream = new BufferedInputStream(new FileInputStream(location));
                }

                Properties p = new Properties();
                try {
                    p.load(inputStream);
                } finally {
                    inputStream.close();
                }

                for (Map.Entry<Object, Object> entry : p.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    String value = String.valueOf(entry.getValue());
                    environmentCopy.put(key, value);
                }
            }
        } catch (IOException ioe) {
            NamingException ne = new NamingException("Unable to load property file: " + location + ".");
            ne.initCause(ioe);
            throw ne;
        }

        // Now inspect the environment and create the bindings for the context
        Map<String, Object> bindings = new ConcurrentHashMap<String, Object>();
        createConnectionFactories(environmentCopy, bindings);
        createQueues(environmentCopy, bindings);
        createTopics(environmentCopy, bindings);

        // Add sub-contexts for dynamic creation on lookup.
        // "dynamicQueues/<queue-name>"
        bindings.put("dynamicQueues", new LazyCreateContext() {
            private static final long serialVersionUID = 6503881346214855588L;

            @Override
            protected Object createEntry(String name) {
                return new JmsQueue(name);
            }
        });

        // "dynamicTopics/<topic-name>"
        bindings.put("dynamicTopics", new LazyCreateContext() {
            private static final long serialVersionUID = 2019166796234979615L;

            @Override
            protected Object createEntry(String name) {
                return new JmsTopic(name);
            }
        });

        return createContext(environmentCopy, bindings);
    }

    private void createConnectionFactories(Hashtable<Object, Object> environment, Map<String, Object> bindings) throws NamingException {
        List<String> names = getConnectionFactoryNames(environment);
        Map<String, String> defaults = getConnectionFactoryDefaults(environment);
        for (String name : names) {
            JmsConnectionFactory factory = null;

            try {
                factory = createConnectionFactory(name, defaults, environment);
            } catch (Exception e) {
                NamingException ne = new NamingException("Exception while creating ConnectionFactory '" + name + "'.");
                ne.initCause(e);
                throw ne;
            }

            bindings.put(name, factory);
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    protected ReadOnlyContext createContext(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        return new ReadOnlyContext(environment, bindings);
    }

    protected JmsConnectionFactory createConnectionFactory(String name, Map<String, String> defaults, Hashtable<Object, Object> environment) throws URISyntaxException {
        String cfNameKey = CONNECTION_FACTORY_KEY_PREFIX + name;
        Map<String, String> props = new LinkedHashMap<String, String>();

        // Add the defaults which apply to all connection factories
        props.putAll(defaults);

        // Add any URI entry for this specific factory name
        Object o = environment.get(cfNameKey);
        if (o != null) {
            String value = String.valueOf(o);
            if (value.trim().length() != 0) {
                props.put(JmsConnectionFactory.REMOTE_URI_PROP, value);
            }
        }

        // Add any factory-specific additional properties
        props.putAll(getConnectionFactoryProperties(name, environment));

        return createConnectionFactory(props);
    }

    protected List<String> getConnectionFactoryNames(Map<Object, Object> environment) {
        List<String> list = new ArrayList<String>();
        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.startsWith(CONNECTION_FACTORY_KEY_PREFIX)) {
                String jndiName = key.substring(CONNECTION_FACTORY_KEY_PREFIX.length());
                list.add(jndiName);
            }
        }

        if(list.isEmpty())
        {
            list.addAll(Arrays.asList(DEFAULT_CONNECTION_FACTORY_NAMES));
        }

        return list;
    }

    protected Map<String, String> getConnectionFactoryDefaults(Map<Object, Object> environment) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        map.put(JmsConnectionFactory.REMOTE_URI_PROP, JmsConnectionFactory.getDefaultRemoteAddress());

        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.startsWith(CONNECTION_FACTORY_DEFAULT_KEY_PREFIX)) {
                String jndiName = key.substring(CONNECTION_FACTORY_DEFAULT_KEY_PREFIX.length());
                map.put(jndiName, String.valueOf(entry.getValue()));
            }
        }

        return Collections.unmodifiableMap(map);
    }

    protected Map<String, String> getConnectionFactoryProperties(String factoryName, Map<Object, Object> environment) {
        Map<String, String> map = new LinkedHashMap<String, String>();

        String factoryPropertiesPrefix = CONNECTION_FACTORY_PROPERTY_KEY_PREFIX + factoryName + ".";

        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.startsWith(factoryPropertiesPrefix)) {
                String propertyName = key.substring(factoryPropertiesPrefix.length());
                map.put(propertyName, String.valueOf(entry.getValue()));
            }
        }

        return map;
    }

    protected void createQueues(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(QUEUE_KEY_PREFIX)) {
                String jndiName = key.substring(QUEUE_KEY_PREFIX.length());
                bindings.put(jndiName, createQueue(entry.getValue().toString()));
            }
        }
    }

    protected void createTopics(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(TOPIC_KEY_PREFIX)) {
                String jndiName = key.substring(TOPIC_KEY_PREFIX.length());
                bindings.put(jndiName, createTopic(entry.getValue().toString()));
            }
        }
    }

    /**
     * Factory method to create new Queue instances
     */
    protected Queue createQueue(String name) {
        return new JmsQueue(name);
    }

    /**
     * Factory method to create new Topic instances
     */
    protected Topic createTopic(String name) {
        return new JmsTopic(name);
    }

    /**
     * Factory method to create a new connection factory using the given properties
     */
    protected JmsConnectionFactory createConnectionFactory(Map<String, String> properties) {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        Map<String, String> unused = factory.setProperties(properties);
        if (!unused.isEmpty()) {
            String msg =
                  " Not all properties could be set on the ConnectionFactory."
                + " Check the properties are spelled correctly."
                + " Unused properties=[" + unused + "].";
            throw new IllegalArgumentException(msg);
        }

        return factory;
    }
}
