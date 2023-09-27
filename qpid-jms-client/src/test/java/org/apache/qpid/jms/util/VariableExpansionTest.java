/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.jms.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.apache.qpid.jms.util.VariableExpansion.Resolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

public class VariableExpansionTest extends QpidJmsTestCase {

    // Environment variable name+value for test, configured in Surefire config
    private static final String TEST_ENV_VARIABLE_NAME = "VAR_EXPANSION_TEST_ENV_VAR";
    private static final String TEST_ENV_VARIABLE_VALUE = "TestEnvVariableValue123";

    private static final String TEST_ENV_VARIABLE_NAME_NOT_SET = "VAR_EXPANSION_TEST_ENV_VAR_NOT_SET";
    private static final String ESCAPE = "$";
    private static final String DEFAULT_DELIMINATOR = ":-";

    private String testNamePrefix;
    private String testPropName;
    private String testPropValue;
    private String testVariableForExpansion;

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        testNamePrefix = getTestName() + ".";

        testPropName = testNamePrefix + "myPropKey";
        testPropValue = testNamePrefix + "myPropValue";
        testVariableForExpansion = "${" + testPropName + "}";
    }

    // ===== Resolver tests =====

    @Test
    public void testResolveWithSysPropResolver() {
        Resolver sysPropResolver = VariableExpansion.SYS_PROP_RESOLVER;

        assertNull(System.getProperty(testPropName), "System property value unexpectedly set already");
        assertNull(sysPropResolver.resolve(testPropName), "Expected resolve to return null as property not set");

        setTestSystemProperty(testPropName, testPropValue);

        assertEquals(testPropValue, System.getProperty(testPropName), "System property value not as expected");
        assertEquals(testPropValue, sysPropResolver.resolve(testPropName), "Resolved variable not as expected");
    }

    @Test
    public void testResolveWithEnvVarResolver() {
        // Verify variable is set (by Surefire config),
        // prevents spurious failure if not manually configured when run in IDE.
        assumeTrue(System.getenv().containsKey(TEST_ENV_VARIABLE_NAME), "Environment variable not set as required");
        assumeFalse(System.getenv().containsKey(TEST_ENV_VARIABLE_NAME_NOT_SET), "Environment variable unexpectedly set");

        assertEquals(TEST_ENV_VARIABLE_VALUE, System.getenv(TEST_ENV_VARIABLE_NAME), "Environment variable value not as expected");

        final Resolver envVarResolver = VariableExpansion.ENV_VAR_RESOLVER;

        assertNull(envVarResolver.resolve(TEST_ENV_VARIABLE_NAME_NOT_SET), "Expected resolve to return null as property not set");

        assertEquals(TEST_ENV_VARIABLE_VALUE, envVarResolver.resolve(TEST_ENV_VARIABLE_NAME), "Resolved variable not as expected");
    }

    // ===== Expansion tests =====

    @Test
    public void testExpandWithResolverNotProvided() {
        try {
            VariableExpansion.expand("no-expansion-needed", null);
            fail("Should have failed to expand,resolver not given");
        } catch (NullPointerException npe) {
            // Expected
        }
    }

    @Test
    public void testExpandNull() {
        assertNull(VariableExpansion.expand(null, variable -> "foo"), "Expected null");
    }

    @Test
    public void testExpandWithSysPropResolver() {
        final Resolver resolver = VariableExpansion.SYS_PROP_RESOLVER;

        try {
            VariableExpansion.expand(testVariableForExpansion, resolver);
            fail("Should have failed to expand, property not set");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        setTestSystemProperty(testPropName, testPropValue);

        assertEquals(testPropValue, System.getProperty(testPropName), "System property value not as expected");

        String expanded = VariableExpansion.expand(testVariableForExpansion, resolver);
        assertEquals(testPropValue, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandWithEnvVarResolver() {
        // Verify variable is set (by Surefire config),
        // prevents spurious failure if not manually configured when run in IDE.
        assumeTrue(System.getenv().containsKey(TEST_ENV_VARIABLE_NAME), "Environment variable not set as required");

        assertEquals(TEST_ENV_VARIABLE_VALUE, System.getenv(TEST_ENV_VARIABLE_NAME), "Environment variable value not as expected");

        final Resolver resolver = VariableExpansion.ENV_VAR_RESOLVER;

        try {
            VariableExpansion.expand("${" + TEST_ENV_VARIABLE_NAME + "_NOT_SET" + "}", resolver);
            fail("Should have failed to expand unset env variable");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        String expanded = VariableExpansion.expand("${" + TEST_ENV_VARIABLE_NAME + "}", resolver);

        assertEquals(TEST_ENV_VARIABLE_VALUE, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandBasicWithMapResolver() {
        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(testPropName, testPropValue);
        Resolver resolver = new VariableExpansion.MapResolver(propsMap);

        try {
            VariableExpansion.expand("${" + testNamePrefix + "-not-set" + "}", resolver);
            fail("Should have failed to expand, property not set");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        String expanded = VariableExpansion.expand(testVariableForExpansion, resolver);

        assertEquals(testPropValue, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandBasic() {
        // Variable is the full input
        doBasicExpansionTestImpl(testVariableForExpansion, testPropValue);

        // Variable trails a prefix
        String prefix = "prefix";
        doBasicExpansionTestImpl(prefix + testVariableForExpansion, prefix + testPropValue);

        // Variable precedes a suffix
        String suffix = "suffix";
        doBasicExpansionTestImpl(testVariableForExpansion + suffix, testPropValue + suffix);

        // Variable is between prefix and suffix
        doBasicExpansionTestImpl(prefix + testVariableForExpansion + suffix, prefix + testPropValue + suffix);
    }

    @Test
    public void testExpandMultipleVariables() {
        String propName2 = "propName2";
        String propValue2 = "propValue2";
        String propName3 = "propName3";
        String propValue3 = "propValue3";

        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(testPropName, testPropValue);
        propsMap.put(propName2, propValue2);
        propsMap.put(propName3, propValue3);

        Resolver resolver = new VariableExpansion.MapResolver(propsMap);

        // Variables are the full input
        String toExpand = testVariableForExpansion + "${" + propName2 +"}${" + propName3 + "}";
        String expected = testPropValue + propValue2 + propValue3;

        doBasicExpansionTestImpl(toExpand, expected, resolver);

        // Variable internal to overall input
        toExpand = "prefix" + testVariableForExpansion + "-foo-${" + propName2 +"}-bar-${" + propName3 + "}" + "suffix";
        expected = "prefix" + testPropValue + "-foo-" + propValue2 +"-bar-" + propValue3 + "suffix";

        doBasicExpansionTestImpl(toExpand, expected, resolver);
    }

    @Test
    public void testExpandMultipleInstancesOfVariable() {
        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(testPropName, testPropValue);

        Resolver resolver = new VariableExpansion.MapResolver(propsMap);

        String toExpand = "1-" + testVariableForExpansion + "2-" + testVariableForExpansion + "3-" + testVariableForExpansion;
        String expected = "1-" + testPropValue + "2-" + testPropValue + "3-" + testPropValue;

        doBasicExpansionTestImpl(toExpand, expected, resolver);
    }

    @Test
    public void testExpandRecursiveThrows() {
        String propName1 = "propName1";
        String propName2 = "propName2";
        String propValue1 = "propValue1-${" + propName2 + "}";
        String propValue2 = "recursive-${" + propName1 + "}";

        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(propName1, propValue1);
        propsMap.put(propName2, propValue2);
        try {
            VariableExpansion.expand("${" + propName1 + "}", new VariableExpansion.MapResolver(propsMap));
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testExpandWithoutVariable() {
        doBasicExpansionTestImpl("no-expansion-needed", "no-expansion-needed");
        doBasicExpansionTestImpl(ESCAPE + "no-expansion-needed", ESCAPE + "no-expansion-needed");
        doBasicExpansionTestImpl("no-expansion-needed" + ESCAPE, "no-expansion-needed" + ESCAPE);
        doBasicExpansionTestImpl(ESCAPE + "no-expansion-needed" + ESCAPE, ESCAPE + "no-expansion-needed" + ESCAPE);
        doBasicExpansionTestImpl("no" + ESCAPE + "-expansion-needed", "no" + ESCAPE + "-expansion-needed");
    }

    @Test
    public void testExpandSkipsEscapedVariables() {
        doBasicExpansionTestImpl(ESCAPE + testVariableForExpansion, testVariableForExpansion);

        String prefix = "prefix";
        doBasicExpansionTestImpl(prefix + ESCAPE + testVariableForExpansion, prefix + testVariableForExpansion);

        String suffix = "suffix";
        doBasicExpansionTestImpl(ESCAPE + testVariableForExpansion + suffix, testVariableForExpansion + suffix);
 
        doBasicExpansionTestImpl(prefix + ESCAPE + testVariableForExpansion + suffix, prefix + testVariableForExpansion + suffix);
    }


    private void doBasicExpansionTestImpl(String toExpand, String expectedExpansion) {
        final Resolver mockResolver = Mockito.mock(Resolver.class);

        Mockito.when(mockResolver.resolve(testPropName)).thenReturn(testPropValue);

        doBasicExpansionTestImpl(toExpand, expectedExpansion, mockResolver);
    }

    private void doBasicExpansionTestImpl(String toExpand, String expectedExpansion, Resolver resolver) {
        String expanded = VariableExpansion.expand(toExpand, resolver);
        assertEquals(expectedExpansion, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandFailsToResolveThrows() {
        final Resolver mockResolver = Mockito.mock(Resolver.class);

        // Check when resolution fails
        Mockito.when(mockResolver.resolve(testPropName)).thenReturn(null);
        try {
            VariableExpansion.expand(testVariableForExpansion, mockResolver);
            fail("Should have failed to expand, property not resolve");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        Mockito.verify(mockResolver).resolve(testPropName);
        Mockito.verifyNoMoreInteractions(mockResolver);
    }

    @Test
    public void testExpandWithUnknownVariableWithDefault() {
        final Resolver mockResolver = Mockito.mock(Resolver.class);

        Mockito.when(mockResolver.resolve(testPropName)).thenReturn(null);
        String defaultValue = "defauledValue" + getTestName();
        String expanded = VariableExpansion.expand("${" + testPropName + DEFAULT_DELIMINATOR + defaultValue + "}", mockResolver);

        Mockito.verify(mockResolver).resolve(testPropName);
        Mockito.verifyNoMoreInteractions(mockResolver);

        assertEquals(defaultValue, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandWithVariableIndirectedToAnotherVariableWithDefault() {
        String propName1 = "propName1";
        String otherPropWhichDoesntExist = "propWhichDoesntExist";
        String otherPropDefault = "defaultValue" + getTestName();
        String propValue1 = "${" + otherPropWhichDoesntExist + DEFAULT_DELIMINATOR + otherPropDefault + "}";

        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(propName1, propValue1);

        VariableExpansion.MapResolver resolver = new VariableExpansion.MapResolver(propsMap);
        assertNull(resolver.resolve(otherPropWhichDoesntExist));

        String expanded = VariableExpansion.expand("${" + propName1 + "}", resolver);

        assertEquals(otherPropDefault, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandWithVariableWithDefaultIndirectedToAnUnknownVariableWithoutDefault() {
        String propName1 = "propName1";
        String prop1Default = "defaultValue" + getTestName();
        String otherPropWhichDoesntExist = "propWhichDoesntExist";
        String propValue1 = "${" + otherPropWhichDoesntExist + "}";

        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(propName1, propValue1);

        VariableExpansion.MapResolver resolver = new VariableExpansion.MapResolver(propsMap);
        assertNull(resolver.resolve(otherPropWhichDoesntExist));

        String expanded = VariableExpansion.expand("${" + propName1 + DEFAULT_DELIMINATOR + prop1Default + "}", resolver);

        assertEquals(prop1Default, expanded, "Expanded variable not as expected");
    }

    @Test
    public void testExpandRecursiveWithDefaultStillThrows() {
        String propName1 = "propName1";
        String defaultValue = "defaultValue" + getTestName();
        String propName2 = "propName2";
        String propValue1 = "propValue1-${" + propName2 + "}";
        String propValue2 = "recursive-${" + propName1 + "}";

        Map<String,String> propsMap = new HashMap<>();
        propsMap.put(propName1, propValue1);
        propsMap.put(propName2, propValue2);
        try {
            VariableExpansion.expand("${" + propName1 + DEFAULT_DELIMINATOR + defaultValue + "}", new VariableExpansion.MapResolver(propsMap));
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }
}
