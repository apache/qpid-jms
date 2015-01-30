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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    static final String DEFAULT_REMOTE_URI = "amqp://localhost:5672";

    private String connectionFactoryPrefix = "connectionfactory.";
    private String queuePrefix = "queue.";
    private String topicPrefix = "topic.";

    @SuppressWarnings("unchecked")
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        // Copy the environment to ensure we don't modify/reference it, it belongs to the caller.
        Hashtable<Object, Object> environmentCopy = new Hashtable<Object, Object>();
        environmentCopy.putAll(environment);

        Map<String, Object> bindings = new ConcurrentHashMap<String, Object>();
        createConnectionFactories(environmentCopy, DEFAULT_REMOTE_URI, bindings);
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

    private void createConnectionFactories(Hashtable<Object, Object> environment, String defaultRemoteURI, Map<String, Object> bindings) throws NamingException {
        List<String> names = getConnectionFactoryNames(environment);
        for (String name : names) {
            JmsConnectionFactory factory = null;

            try {
                factory = createConnectionFactory(name, defaultRemoteURI, environment);
            } catch (Exception e) {
                throw new NamingException("Invalid ConnectionFactory definition");
            }

            bindings.put(name, factory);
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    protected ReadOnlyContext createContext(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        return new ReadOnlyContext(environment, bindings);
    }

    protected JmsConnectionFactory createConnectionFactory(String name, String defaultRemoteURI, Hashtable<Object, Object> environment) throws URISyntaxException {
        String cfNameKey = connectionFactoryPrefix + name;
        Map<String, String> props = new LinkedHashMap<String, String>();

        // Use the default URI if none is defined for this factory in the environment
        String uri = defaultRemoteURI;
        if (environment.containsKey(cfNameKey)) {
            uri = String.valueOf(environment.get(cfNameKey));
        }

        props.put(JmsConnectionFactory.REMOTE_URI_PROP, uri);

        //TODO: support gathering up any other per-factory properties from the environment

        return createConnectionFactory(props);
    }

    protected List<String> getConnectionFactoryNames(Map<Object, Object> environment) {
        List<String> list = new ArrayList<String>();
        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = String.valueOf(entry.getKey());
            if (key.startsWith(connectionFactoryPrefix)) {
                String jndiName = key.substring(connectionFactoryPrefix.length());
                list.add(jndiName);
            }
        }

        if(list.isEmpty())
        {
            list.addAll(Arrays.asList(DEFAULT_CONNECTION_FACTORY_NAMES));
        }

        return list;
    }

    protected void createQueues(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(queuePrefix)) {
                String jndiName = key.substring(queuePrefix.length());
                bindings.put(jndiName, createQueue(entry.getValue().toString()));
            }
        }
    }

    protected void createTopics(Hashtable<Object, Object> environment, Map<String, Object> bindings) {
        for (Iterator<Entry<Object, Object>> iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Object, Object> entry = iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(topicPrefix)) {
                String jndiName = key.substring(topicPrefix.length());
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
    protected JmsConnectionFactory createConnectionFactory(Map<String, String> properties) throws URISyntaxException {
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.setProperties(properties);
        return factory;
    }
}
