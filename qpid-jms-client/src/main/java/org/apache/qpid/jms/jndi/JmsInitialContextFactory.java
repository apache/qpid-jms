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
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
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

    private static final String[] DEFAULT_CONNECTION_FACTORY_NAMES = {
        "ConnectionFactory", "QueueConnectionFactory", "TopicConnectionFactory" };

    private String connectionFactoryPrefix = "connectionfactory.";
    private String queuePrefix = "queue.";
    private String topicPrefix = "topic.";

    @Override
    public Context getInitialContext(Hashtable environment) throws NamingException {
        // lets create a factory
        Map<String, Object> data = new ConcurrentHashMap<String, Object>();
        String[] names = getConnectionFactoryNames(environment);
        for (int i = 0; i < names.length; i++) {
            JmsConnectionFactory factory = null;
            String name = names[i];

            try {
                factory = createConnectionFactory(name, environment);
            } catch (Exception e) {
                throw new NamingException("Invalid broker URL");
            }

            data.put(name, factory);
        }

        createQueues(data, environment);
        createTopics(data, environment);

        // Add sub-contexts for dynamic creation on lookup.
        // "dynamicQueues/<queue-name>"
        data.put("dynamicQueues", new LazyCreateContext() {
            private static final long serialVersionUID = 6503881346214855588L;

            @Override
            protected Object createEntry(String name) {
                return new JmsQueue(name);
            }
        });

        // "dynamicTopics/<topic-name>"
        data.put("dynamicTopics", new LazyCreateContext() {
            private static final long serialVersionUID = 2019166796234979615L;

            @Override
            protected Object createEntry(String name) {
                return new JmsTopic(name);
            }
        });

        return createContext(environment, data);
    }

    static private String getValue(Hashtable environment, String key, String defaultValue) {
        Object o = environment.get(key);
        if (o != null && o instanceof String) {
            return (String) o;
        } else {
            return defaultValue;
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    protected ReadOnlyContext createContext(Hashtable environment, Map<String, Object> data) {
        return new ReadOnlyContext(environment, data);
    }

    protected JmsConnectionFactory createConnectionFactory(String name, Hashtable environment) throws URISyntaxException {
        Hashtable temp = new Hashtable(environment);
        String prefix = connectionFactoryPrefix + name + ".";
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                // Rename the key...
                temp.remove(key);
                key = key.substring(prefix.length());
                temp.put(key, entry.getValue());
            }
        }
        return createConnectionFactory(temp);
    }

    protected String[] getConnectionFactoryNames(Map environment) {
        String factoryNames = (String) environment.get("factories");
        if (factoryNames != null) {
            List<String> list = new ArrayList<String>();
            for (StringTokenizer enumeration = new StringTokenizer(factoryNames, ","); enumeration.hasMoreTokens();) {
                list.add(enumeration.nextToken().trim());
            }
            int size = list.size();
            if (size > 0) {
                String[] answer = new String[size];
                list.toArray(answer);
                return answer;
            }
        }
        return DEFAULT_CONNECTION_FACTORY_NAMES;
    }

    protected void createQueues(Map<String, Object> data, Hashtable environment) {
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(queuePrefix)) {
                String jndiName = key.substring(queuePrefix.length());
                data.put(jndiName, createQueue(entry.getValue().toString()));
            }
        }
    }

    protected void createTopics(Map<String, Object> data, Hashtable environment) {
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(topicPrefix)) {
                String jndiName = key.substring(topicPrefix.length());
                data.put(jndiName, createTopic(entry.getValue().toString()));
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
     * Factory method to create a new connection factory from the given
     * environment
     */
    protected JmsConnectionFactory createConnectionFactory(Hashtable<Object,Object> environment) throws URISyntaxException {
        JmsConnectionFactory answer = new JmsConnectionFactory();
        Map<String, String> properties = toMap(environment);
        properties.remove(Context.INITIAL_CONTEXT_FACTORY);
        Object o = properties.remove(Context.PROVIDER_URL);
        if (o != null) {
            answer.setBrokerURI(o.toString());
        }
        answer.setProperties(properties);
        return answer;
    }

    public Map<String, String> toMap(Hashtable<Object, Object> props) {
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            map.put(entry.getKey().toString(), entry.getValue().toString());
        }
        return map;
    }
}
