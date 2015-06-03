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
package org.apache.qpid.jms.provider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.URI;

import org.apache.qpid.jms.provider.mock.MockProvider;
import org.apache.qpid.jms.provider.mock.MockProviderFactory;
import org.apache.qpid.jms.test.QpidJmsTestCase;
import org.junit.After;
import org.junit.Test;

public class ProviderWrapperTest extends QpidJmsTestCase{

    MockProvider mockProvider = null;

    @Override
    @After
    public void tearDown() throws Exception {
        if (mockProvider != null) {
            mockProvider.close();
        }
        super.tearDown();
    }

    @Test
    public void testGetLocalPrincipal() throws Exception {
        String principalName = "foo";

        MockProviderFactory factory = new MockProviderFactory();
        mockProvider = factory.createProvider(new URI("mock://1.2.3.4:5678?mock.localPrincipal=" + principalName));

        assertNotNull(mockProvider.getLocalPrincipal());
        assertEquals(principalName, mockProvider.getLocalPrincipal().getName());

        ProviderWrapper<MockProvider> wrapper = new ProviderWrapper<MockProvider>(mockProvider);

        assertNotNull(wrapper.getLocalPrincipal());
        assertEquals(principalName, wrapper.getLocalPrincipal().getName());
    }

    @Test
    public void testGetLocalPrincipalNull() throws Exception {
        MockProviderFactory factory = new MockProviderFactory();
        mockProvider = factory.createProvider(new URI("mock://1.2.3.4:5678"));

        assertNull(mockProvider.getLocalPrincipal());

        ProviderWrapper<MockProvider> wrapper = new ProviderWrapper<MockProvider>(mockProvider);

        assertNull(wrapper.getLocalPrincipal());
    }
}
