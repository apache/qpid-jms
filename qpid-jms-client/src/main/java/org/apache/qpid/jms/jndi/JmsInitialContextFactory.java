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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    private String connectionPrefix = "";

    String queuePrefix = "/queue/";
    String topicPrefix = "/topic/";

    @Override
    public Context getInitialContext(Hashtable environment) throws NamingException {

        queuePrefix = getValue(environment, "queuePrefix", queuePrefix);
        topicPrefix = getValue(environment, "topicPrefix", topicPrefix);

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

        data.put("queue", new LazyCreateContext() {
            private static final long serialVersionUID = 6503881346214855588L;

            @Override
            protected Object createEntry(String name) {
                return new JmsQueue(name);
            }
        });

        data.put("topic", new LazyCreateContext() {
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
        String prefix = connectionPrefix + name + ".";
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
    protected JmsConnectionFactory createConnectionFactory(Hashtable environment) throws URISyntaxException {
        JmsConnectionFactory answer = new JmsConnectionFactory();
        Properties properties = new Properties();
        environment.remove("java.naming.factory.initial");
        Object o = environment.remove("java.naming.provider.url");
        if (o != null) {
            answer.setBrokerURI(o.toString());
        }
        o = environment.remove("java.naming.security.principal");
        if (o != null) {
            answer.setUsername(o.toString());
        }
        o = environment.remove("java.naming.security.credentials");
        if (o != null) {
            answer.setPassword(o.toString());
        }
        properties.putAll(environment);
        answer.setProperties(properties);
        return answer;
    }

    public String getConnectionPrefix() {
        return connectionPrefix;
    }

    public void setConnectionPrefix(String connectionPrefix) {
        this.connectionPrefix = connectionPrefix;
    }
}
