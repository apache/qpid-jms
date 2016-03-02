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
package org.apache.qpid.jms.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test basic functionality of class FactoryFinder
 */
public class FactoryFinderTest {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryFinderTest.class);

    public static final String BASE_PATH = "/src/test/resources/factory/";

    private String TEST_LOCATOR_PATH;

    public interface TestFactory {
        String create();
    }

    public class TestFactoryReturnsFOO implements TestFactory {

        @Override
        public String create() {
            return "FOO";
        }
    }

    public class TestFactoryReturnsBAR implements TestFactory {

        @Override
        public String create() {
            return "BAR";
        }
    }

    public class TestObjectFactory implements FactoryFinder.ObjectFactory {

        @Override
        public Object create(String path) throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
            return null;
        }
    }

    @Before
    public void setUp() throws Exception {
        TEST_LOCATOR_PATH = new File(".").getCanonicalPath() + BASE_PATH;
        LOG.info("Test path is: {}", TEST_LOCATOR_PATH);
    }

    @Test
    public void testFactoryFinderCreate() {
        new FactoryFinder<TestFactory>(TestFactory.class, TEST_LOCATOR_PATH);
    }

    @Test
    public void testGetObjectFactory() {
        assertNotNull(FactoryFinder.getObjectFactory());
    }

    @Test
    public void testSetObjectFactory() {
        TestObjectFactory testFactory = new TestObjectFactory();
        assertNotNull(FactoryFinder.getObjectFactory());
        assertNotSame(testFactory, FactoryFinder.getObjectFactory());
        FactoryFinder.ObjectFactory oldObjectFactory = FactoryFinder.getObjectFactory();
        FactoryFinder.setObjectFactory(testFactory);
        assertSame(testFactory, FactoryFinder.getObjectFactory());
        FactoryFinder.setObjectFactory(oldObjectFactory);
    }

    @Test
    public void testRegisterProviderFactory() throws Exception {
        FactoryFinder<TestFactory> factory = new FactoryFinder<TestFactory>(TestFactory.class, TEST_LOCATOR_PATH);
        factory.registerProviderFactory("bar", new TestFactoryReturnsBAR());

        TestFactory barFactory = factory.newInstance("bar");
        assertNotNull(barFactory);
        assertTrue(barFactory instanceof TestFactoryReturnsBAR);
    }
}
