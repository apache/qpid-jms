/*
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
 */
package org.apache.qpid.jms.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QpidJmsTestCase {
    public static final boolean IS_WINDOWS = System.getProperty("os.name", "unknown").toLowerCase().contains("windows");

    private final Logger _logger = LoggerFactory.getLogger(getClass());

    private final Map<String, String> _propertiesSetForTest = new HashMap<String, String>();

    public String _testMethodName;

    /**
     * Set a System property for duration of this test only. The tearDown will guarantee to reset the property to its
     * previous value after the test completes.
     *
     * @param property
     *            The property to set
     * @param value
     *            the value to set it to, if null, the property will be cleared
     */
    protected void setTestSystemProperty(final String property, final String value) {
        if (!_propertiesSetForTest.containsKey(property)) {
            // Record the current value so we can revert it later.
            _propertiesSetForTest.put(property, System.getProperty(property));
        }

        if (value == null) {
            System.clearProperty(property);
            _logger.info("Set system property '" + property + "' to be cleared");
        } else {
            System.setProperty(property, value);
            _logger.info("Set system property '" + property + "' to: '" + value + "'");
        }

    }

    /**
     * Restore the System property values that were set by this test run.
     */
    protected void revertTestSystemProperties() {
        if (!_propertiesSetForTest.isEmpty()) {
            for (String key : _propertiesSetForTest.keySet()) {
                String value = _propertiesSetForTest.get(key);
                if (value != null) {
                    System.setProperty(key, value);
                    _logger.info("Reverted system property '" + key + "' to: '" + value + "'");
                } else {
                    System.clearProperty(key);
                    _logger.info("Reverted system property '" + key + "' to be cleared");
                }
            }

            _propertiesSetForTest.clear();
        }
    }

    @AfterEach
    public void tearDown() throws java.lang.Exception {
        _logger.info("========== tearDown " + getTestName() + " ==========");
        revertTestSystemProperties();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        _testMethodName = testInfo.getTestMethod().get().getName();

        _logger.info("========== start " + getTestName() + " ==========");
    }

    protected String getTestName() {
        return getClass().getSimpleName() + "." + _testMethodName;
    }
}
