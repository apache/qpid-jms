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

import static org.junit.Assert.*;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

public class JmsInitialContextFactoryTest extends QpidJmsTestCase {

    private JmsInitialContextFactory factory;
    private Context context;

    private Context createInitialContext(final Hashtable<Object, Object> environment) throws NamingException {
        factory = new JmsInitialContextFactory();
        context = factory.getInitialContext(environment);
        assertNotNull("No context created", context);

        return context;
    }

    @Test
    public void testConnectionFactoryBinding() throws Exception {
        String factoryName = "myNewFactory";
        String uri = "amqp://example.com:1234";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("connectionfactory." + factoryName, uri);
        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(factoryName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
        assertEquals("Unexpected URI for returned factory", ((JmsConnectionFactory) o).getRemoteURI(), uri);
    }

    @Test
    public void testQueueBinding() throws Exception {
        String lookupName = "myLookupName";
        String actualName = "myQueueName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("queue." + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());
        assertEquals("Unexpected name for returned object", ((JmsQueue) o).getQueueName(), actualName);
    }

    @Test
    public void testDynamicQueueLookup() throws Exception {
        String actualName = "myQueueName";
        String lookupName = "dynamicQueues/" + actualName;

        Context ctx = createInitialContext(new Hashtable<Object, Object>());
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());
        assertEquals("Unexpected name for returned object", ((JmsQueue) o).getQueueName(), actualName);
    }

    @Test
    public void testTopicBinding() throws Exception {
        String lookupName = "myLookupName";
        String actualName = "myTopicName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("topic." + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());
        assertEquals("Unexpected name for returned object", ((JmsTopic) o).getTopicName(), actualName);
    }

    @Test
    public void testDynamicTopicLookup() throws Exception {
        String actualName = "myTopicName";
        String lookupName = "dynamicTopics/" + actualName;

        Context ctx = createInitialContext(new Hashtable<Object, Object>());
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());
        assertEquals("Unexpected name for returned object", ((JmsTopic) o).getTopicName(), actualName);
    }

    @Test
    public void testQueueBindingWithSlashInLookupName() throws Exception {
        String lookupName = "myLookup/Name";
        String actualName = "myQueueName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("queue." + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());
        assertEquals("Unexpected name for returned object", ((JmsQueue) o).getQueueName(), actualName);
    }

    @Test
    public void testTopicBindingWithMulupleSlashesInLookupName() throws Exception {
        String lookupName = "my/Lookup/Name";
        String actualName = "myTopicName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("topic." + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());
        assertEquals("Unexpected name for returned object", ((JmsTopic) o).getTopicName(), actualName);
    }

    @Test(expected = OperationNotSupportedException.class)
    public void testContextPreventsUnbind() throws Exception {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        Context ctx = createInitialContext(env);

        ctx.unbind("lookupName");
    }

    @Test(expected = OperationNotSupportedException.class)
    public void testContextPreventsRebind() throws Exception {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        Context ctx = createInitialContext(env);

        ctx.rebind("lookupName", new Object());
    }

    @Test(expected = OperationNotSupportedException.class)
    public void testContextPreventsRename() throws Exception {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        Context ctx = createInitialContext(env);

        ctx.rename("lookupName", "");
    }
}
