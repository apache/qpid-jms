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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.JmsTopic;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.Test;

public class JmsInitialContextFactoryTest extends QpidJmsTestCase {

    // Environment variable name+value for test, configured in Surefire config
    private static final String TEST_ENV_VARIABLE_NAME = "VAR_EXPANSION_TEST_ENV_VAR";
    private static final String TEST_ENV_VARIABLE_VALUE = "TestEnvVariableValue123";

    private static final String DEFAULT_DELIMINATOR = ":-";

    private JmsInitialContextFactory factory;
    private Context context;

    private Context createInitialContext(final Hashtable<Object, Object> environment) throws NamingException {
        factory = new JmsInitialContextFactory();
        context = factory.getInitialContext(environment);
        assertNotNull("No context created", context);

        return context;
    }

    @Test
    public void testDefaultConnectionFactoriesPresentWithEmptyEnvironment() throws Exception {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        Context ctx = createInitialContext(env);

        for (String factoryName : JmsInitialContextFactory.DEFAULT_CONNECTION_FACTORY_NAMES) {
            Object o = ctx.lookup(factoryName);

            assertNotNull("No object returned", o);
            assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
            assertEquals("Unexpected URI for returned factory", JmsConnectionFactory.getDefaultRemoteAddress(), ((JmsConnectionFactory) o).getRemoteURI());
        }
    }

    @Test
    public void testDefaultConnectionFactoriesSeeDefaultURIUpdate() throws Exception {
        String updatedDefaultURI = "amqp://example.com:1234";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.CONNECTION_FACTORY_DEFAULT_KEY_PREFIX + JmsConnectionFactory.REMOTE_URI_PROP, updatedDefaultURI);
        Context ctx = createInitialContext(env);

        for (String factoryName : JmsInitialContextFactory.DEFAULT_CONNECTION_FACTORY_NAMES) {
            Object o = ctx.lookup(factoryName);

            assertNotNull("No object returned", o);
            assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
            assertEquals("Unexpected URI for returned factory", updatedDefaultURI, ((JmsConnectionFactory) o).getRemoteURI());
        }
    }

    @Test
    public void testDefaultConnectionFactorySeesFactorySpecificProperty() throws Exception {
        String factoryName = JmsInitialContextFactory.DEFAULT_CONNECTION_FACTORY_NAMES[0];

        // lower case prefix
        doDefaultConnectionFactorySeesFactorySpecificPropertyTestImpl("property.connectionfactory.", factoryName);
        // camelCase prefix
        doDefaultConnectionFactorySeesFactorySpecificPropertyTestImpl("property.connectionFactory.", factoryName);
    }

    private void doDefaultConnectionFactorySeesFactorySpecificPropertyTestImpl(String propertyPrefix, String factoryName) throws Exception {
        String updatedClientID = _testName.getMethodName();

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(propertyPrefix + factoryName + "." + "clientID", updatedClientID);
        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(factoryName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
        assertEquals("Unexpected ClientID for returned factory", updatedClientID, ((JmsConnectionFactory) o).getClientID());
    }

    @Test
    public void testDefaultConnectionFactoriesNotPresentWhenOneIsExplicitlyDefined() throws Exception {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.CONNECTION_FACTORY_KEY_PREFIX + "myNewFactory", "amqp://example.com:1234");
        Context ctx = createInitialContext(env);

        for (String factoryName : JmsInitialContextFactory.DEFAULT_CONNECTION_FACTORY_NAMES) {
            try {
                ctx.lookup(factoryName);
                fail("should have thrown exception due to name not being found");
            } catch (NameNotFoundException nnfe) {
                // //expected
            }
        }
    }


    @Test
    public void testDefaultConnectionFactoriesSeeDefaultPropertyUpdate() throws Exception {
        String factoryName = JmsInitialContextFactory.DEFAULT_CONNECTION_FACTORY_NAMES[0];

        // lower case prefix
        doDefaultConnectionFactorySeesDefaultPropertyUpdatePropertyTestImpl("default.connectionfactory.", factoryName);
        // camelCase prefix
        doDefaultConnectionFactorySeesDefaultPropertyUpdatePropertyTestImpl("default.connectionFactory.", factoryName);
    }

    private void doDefaultConnectionFactorySeesDefaultPropertyUpdatePropertyTestImpl(String propertyPrefix, String factoryName) throws Exception {
        String updatedClientID = _testName.getMethodName();

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(propertyPrefix + "clientID", updatedClientID);
        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(factoryName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
        assertEquals("Unexpected ClientID for returned factory", updatedClientID, ((JmsConnectionFactory) o).getClientID());
    }

    @Test
    public void testConnectionFactoryBinding() throws Exception {
        String factoryName = "myNewFactory";
        // lower case prefix
        doConnectionFactoryBindingTestImpl("connectionfactory." + factoryName, factoryName);
        // camelCase prefix
        doConnectionFactoryBindingTestImpl("connectionFactory." + factoryName, factoryName);
    }

    private void doConnectionFactoryBindingTestImpl(String environmentProperty, String factoryName) throws NamingException {
        String uri = "amqp://example.com:1234";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(environmentProperty, uri);
        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(factoryName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
        assertEquals("Unexpected URI for returned factory", uri, ((JmsConnectionFactory) o).getRemoteURI());
    }

    @Test
    public void testConnectionFactoryBindingWithInvalidFactorySpecificProperty() throws Exception {
        String factoryName = "myNewFactory";
        String uri = "amqp://example.com:1234";

        String propertyPrefix = JmsInitialContextFactory.CONNECTION_FACTORY_PROPERTY_KEY_PREFIX;

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.CONNECTION_FACTORY_KEY_PREFIX + factoryName, uri);
        env.put(propertyPrefix + factoryName + "." + "invalidProperty", "value");

        try {
            createInitialContext(env);
            fail("Should have thrown exception");
        } catch (NamingException ne) {
            // Expected
            assertTrue("Should have had a cause", ne.getCause() != null);
        }
    }

    @Test
    public void testConnectionFactoryBindingWithInvalidDefaultProperty() throws Exception {
        String factoryName = "myNewFactory";
        String uri = "amqp://example.com:1234";

        String defaultPrefix = JmsInitialContextFactory.CONNECTION_FACTORY_DEFAULT_KEY_PREFIX;

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.CONNECTION_FACTORY_KEY_PREFIX + factoryName, uri);
        env.put(defaultPrefix + "invalidDefaultProperty", "value");

        try {
            createInitialContext(env);
            fail("Should have thrown exception");
        } catch (NamingException ne) {
            // Expected
            assertTrue("Should have had a cause", ne.getCause() != null);
        }
    }

    @Test
    public void testConnectionFactoryBindingUsesDefaultURIWhenEmpty() throws Exception {
        doConnectionFactoryBindingUsesDefaultURITestImpl("");
    }

    @Test
    public void testConnectionFactoryBindingUsesDefaultURIWhenNull() throws Exception {
        doConnectionFactoryBindingUsesDefaultURITestImpl("");
    }

    private void doConnectionFactoryBindingUsesDefaultURITestImpl(String uriPropertyValue) throws NamingException {
        String factoryName = "myNewFactory";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.CONNECTION_FACTORY_KEY_PREFIX + factoryName, uriPropertyValue);
        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(factoryName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());
        assertEquals("Unexpected URI for returned factory", JmsConnectionFactory.getDefaultRemoteAddress(), ((JmsConnectionFactory) o).getRemoteURI());
    }

    @Test
    public void testQueueBinding() throws Exception {
        String lookupName = "myLookupName";
        String actualName = "myQueueName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.QUEUE_KEY_PREFIX + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());
        assertEquals("Unexpected name for returned object", actualName, ((JmsQueue) o).getQueueName());
    }

    @Test
    public void testDynamicQueueLookup() throws Exception {
        String actualName = "myQueueName";
        String lookupName = "dynamicQueues/" + actualName;

        Context ctx = createInitialContext(new Hashtable<Object, Object>());
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());
        assertEquals("Unexpected name for returned object", actualName, ((JmsQueue) o).getQueueName());
    }

    @Test
    public void testTopicBinding() throws Exception {
        String lookupName = "myLookupName";
        String actualName = "myTopicName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.TOPIC_KEY_PREFIX + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());
        assertEquals("Unexpected name for returned object", actualName, ((JmsTopic) o).getTopicName());
    }

    @Test
    public void testDynamicTopicLookup() throws Exception {
        String actualName = "myTopicName";
        String lookupName = "dynamicTopics/" + actualName;

        Context ctx = createInitialContext(new Hashtable<Object, Object>());
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());
        assertEquals("Unexpected name for returned object", actualName, ((JmsTopic) o).getTopicName());
    }

    @Test
    public void testQueueBindingWithSlashInLookupName() throws Exception {
        String lookupName = "myLookup/Name";
        String actualName = "myQueueName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.QUEUE_KEY_PREFIX + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());
        assertEquals("Unexpected name for returned object", actualName, ((JmsQueue) o).getQueueName());
    }

    @Test
    public void testTopicBindingWithMulupleSlashesInLookupName() throws Exception {
        String lookupName = "my/Lookup/Name";
        String actualName = "myTopicName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(JmsInitialContextFactory.TOPIC_KEY_PREFIX + lookupName, actualName);

        Context ctx = createInitialContext(env);
        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());
        assertEquals("Unexpected name for returned object", actualName, ((JmsTopic) o).getTopicName());
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

    @Test
    public void testContextFromProviderUrlNotFoundThrowsNamingException() throws Exception {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();

        env.put(Context.PROVIDER_URL, "/does/not/exist/1234");
        try {
            createInitialContext(env);
            fail("Should have thrown exception");
        } catch (NamingException ne) {
            // Expected
            assertTrue("Should have had a cause", ne.getCause() != null);
        }
    }

    @Test
    public void testContextFromProviderUrlInEnvironmentMap() throws Exception {
        doContextFromProviderUrlInEnvironmentMapTestImpl(false);
    }

    @Test
    public void testContextFromProviderUrlInEnvironmentMapWithBareFilePath() throws Exception {
        doContextFromProviderUrlInEnvironmentMapTestImpl(true);
    }

    private void doContextFromProviderUrlInEnvironmentMapTestImpl(boolean useBareFilePath) throws IOException, FileNotFoundException, NamingException {
        String myFactory = "myFactory";
        String myURI = "amqp://example.com:2765";

        Properties properties = new Properties();
        properties.put("connectionfactory." + myFactory, myURI);

        File f = File.createTempFile(getTestName(), ".properties");
        try {
            FileOutputStream fos = new FileOutputStream(f);
            try {
                properties.store(fos, null);
            } finally {
                fos.close();
            }

            Hashtable<Object, Object> env = new Hashtable<Object, Object>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, JmsInitialContextFactory.class.getName());
            if (useBareFilePath) {
                env.put(Context.PROVIDER_URL, f.getAbsolutePath());
            } else if(QpidJmsTestCase.IS_WINDOWS) {
                env.put(Context.PROVIDER_URL, "file:///" + f.getAbsolutePath());
            } else {
                env.put(Context.PROVIDER_URL, "file://" + f.getAbsolutePath());
            }

            InitialContext context = new InitialContext(env);

            ConnectionFactory factory = (ConnectionFactory) context.lookup(myFactory);
            assertEquals("Unexpected type of object", JmsConnectionFactory.class, factory.getClass());
            assertEquals("Unexpected URI value", myURI, ((JmsConnectionFactory) factory).getRemoteURI());

            context.close();
        } finally {
            f.delete();
        }
    }

    @Test
    public void testContextFromProviderUrlInSystemProperty() throws Exception
    {
        String myFactory = "myFactory";
        String myURI = "amqp://example.com:2765";

        Properties properties = new Properties();
        properties.put("connectionfactory." + myFactory, myURI);

        File f = File.createTempFile(getTestName(), ".properties");
        try {
            FileOutputStream fos = new FileOutputStream(f);
            try {
                properties.store(fos, null);
            } finally {
                fos.close();
            }

            setTestSystemProperty(Context.INITIAL_CONTEXT_FACTORY, JmsInitialContextFactory.class.getName());
            setTestSystemProperty(Context.PROVIDER_URL, "file://" + f.getAbsolutePath());
            if(QpidJmsTestCase.IS_WINDOWS) {
                setTestSystemProperty(Context.PROVIDER_URL, "file:///" + f.getAbsolutePath());
            } else {
                setTestSystemProperty(Context.PROVIDER_URL, "file://" + f.getAbsolutePath());
            }

            InitialContext context = new InitialContext();

            ConnectionFactory factory = (ConnectionFactory) context.lookup(myFactory);
            assertEquals("Unexpected type of object", JmsConnectionFactory.class, factory.getClass());
            assertEquals("Unexpected URI value", myURI, ((JmsConnectionFactory) factory).getRemoteURI());

            context.close();
        } finally {
            f.delete();
        }
    }

    @Test
    public void testVariableExpansionUnresolvableVariable() throws Exception {
        //Check exception is thrown for variable that doesn't resolve
        String factoryName = "myFactory";
        String unknownVariable = "unknownVariable";
        String uri = "amqp://${"+ unknownVariable +"}:1234";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("connectionfactory." + factoryName, uri);

        try {
            createInitialContext(env);
            fail("Expected to fail due to unresolved variable");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        String nowKnownHostValue = "nowKnownValue";

        //Now make the variable resolve, check the exact same env+URI now works
        setTestSystemProperty(unknownVariable, nowKnownHostValue);

        Context ctx = createInitialContext(env);

        Object o = ctx.lookup("myFactory");

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());

        assertEquals("Unexpected URI for returned factory", "amqp://" + nowKnownHostValue + ":1234", ((JmsConnectionFactory) o).getRemoteURI());
    }

    @Test
    public void testVariableExpansionUnresolvableVariableWithDefault() throws Exception {
        // Check exception is not thrown for variable that doesn't resolve when it has a default
        String factoryName = "myFactory";
        String unknownVariable = "unknownVariable";
        String unknownVariableDefault = "default" + getTestName();
        String uri = "amqp://${"+ unknownVariable + DEFAULT_DELIMINATOR + unknownVariableDefault +"}:1234";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("connectionfactory." + factoryName, uri);

        //Verify the default is picked up when the variable doesn't resolve
        Context ctx = createInitialContext(env);
        Object o = ctx.lookup("myFactory");
        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());

        assertEquals("Unexpected URI for returned factory", "amqp://" + unknownVariableDefault + ":1234", ((JmsConnectionFactory) o).getRemoteURI());

        //Now make the variable resolve, check the exact same env+URI now produces different result
        String nowSetHostVarValue = "nowSetHostVarValue";
        setTestSystemProperty(unknownVariable, nowSetHostVarValue);

        ctx = createInitialContext(env);
        o = ctx.lookup("myFactory");
        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());

        assertEquals("Unexpected URI for returned factory", "amqp://" + nowSetHostVarValue + ":1234", ((JmsConnectionFactory) o).getRemoteURI());
    }

    @Test
    public void testVariableExpansionConnectionFactory() throws Exception {
        doVariableExpansionConnectionFactoryTestImpl(false);
    }

    @Test
    public void testVariableExpansionConnectionFactoryWithEnvVar() throws Exception {
        doVariableExpansionConnectionFactoryTestImpl(true);
    }

    private void doVariableExpansionConnectionFactoryTestImpl(boolean useEnvVarForHost) throws NamingException {
        String factoryName = "myFactory";

        String hostVariableName = useEnvVarForHost ? TEST_ENV_VARIABLE_NAME : "myHostVar";
        String portVariableName = "myPortVar";
        String clientIdVariableName = "myClientIDVar";
        String hostVariableValue = useEnvVarForHost ? TEST_ENV_VARIABLE_VALUE : "myHostValue";
        String portVariableValue= "1234";
        String clientIdVariableValue= "myClientIDValue" + getTestName();
        Object environmentProperty = "connectionfactory." + factoryName;

        if(useEnvVarForHost) {
            // Verify variable is set (by Surefire config),
            // prevents spurious failure if not manually configured when run in IDE.
            assertEquals("Expected to use env variable name", TEST_ENV_VARIABLE_NAME, hostVariableName);
            assumeTrue("Environment variable not set as required", System.getenv().containsKey(TEST_ENV_VARIABLE_NAME));
            assertEquals("Environment variable value not as expected", TEST_ENV_VARIABLE_VALUE, System.getenv(TEST_ENV_VARIABLE_NAME));
        } else {
            assertNotEquals("Expected to use a different name", TEST_ENV_VARIABLE_NAME, hostVariableName);

            setTestSystemProperty(hostVariableName, hostVariableValue);
        }
        setTestSystemProperty(portVariableName, portVariableValue);

        String uri = "amqp://${" + hostVariableName + "}:${" + portVariableName + "}?jms.clientID=${" + clientIdVariableName + "}";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(environmentProperty, uri);
        env.put(clientIdVariableName, clientIdVariableValue);

        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(factoryName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsConnectionFactory.class, o.getClass());

        assertEquals("Unexpected ClientID for returned factory", clientIdVariableValue, ((JmsConnectionFactory) o).getClientID());

        String expectedURI = "amqp://" + hostVariableValue + ":" + portVariableValue;
        assertEquals("Unexpected URI for returned factory", expectedURI, ((JmsConnectionFactory) o).getRemoteURI());
    }

    @Test
    public void testVariableExpansionQueue() throws Exception {
        String lookupName = "myQueueLookup";
        String variableName = "myQueueVariable";
        String variableValue = "myQueueName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("queue." + lookupName, "${" + variableName +"}");

        setTestSystemProperty(variableName, variableValue);

        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsQueue.class, o.getClass());

        assertEquals("Unexpected name for returned queue", variableValue, ((JmsQueue) o).getQueueName());
    }

    @Test
    public void testVariableExpansionTopic() throws Exception {
        String lookupName = "myTopicLookup";
        String variableName = "myTopicVariable";
        String variableValue = "myTopicName";

        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put("topic." + lookupName, "${" + variableName +"}");

        setTestSystemProperty(variableName, variableValue);

        Context ctx = createInitialContext(env);

        Object o = ctx.lookup(lookupName);

        assertNotNull("No object returned", o);
        assertEquals("Unexpected class type for returned object", JmsTopic.class, o.getClass());

        assertEquals("Unexpected name for returned queue", variableValue, ((JmsTopic) o).getTopicName());
    }
}
