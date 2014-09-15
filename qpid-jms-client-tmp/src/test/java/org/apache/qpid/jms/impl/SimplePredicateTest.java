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
package org.apache.qpid.jms.impl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.junit.Test;

public class SimplePredicateTest extends QpidJmsTestCase
{
    private static final String DESCRIPTION = "description1";
    private static final String PREDICATED_OBJECT = "predicatedObject1";

    @Test
    public void testGetCurrentStateWithDefaultPredicate()
    {
        SimplePredicate predicate = new SimplePredicate()
        {
            @Override
            public boolean test()
            {
                return true;
            }
        };
        String currentState = predicate.getCurrentState();
        assertThat(currentState, containsString(predicate.getClass().getName()));
    }

    @Test
    public void testGetCurrentStateWithAPredicatedObject()
    {
        SimplePredicate predicate = new SimplePredicate(DESCRIPTION, PREDICATED_OBJECT)
        {
            @Override
            public boolean test()
            {
                return true;
            }
        };

        String currentState = predicate.getCurrentState();
        assertThat(currentState, containsString(DESCRIPTION));
        assertThat(currentState, containsString(PREDICATED_OBJECT));
    }

    @Test
    public void testToStringWithDefaultPredicate()
    {
        SimplePredicate predicate = new SimplePredicate()
        {
            @Override
            public boolean test()
            {
                return true;
            }
        };

        assertThat(predicate.toString(), containsString(predicate.getClass().getName()));
    }

    @Test
    public void testToStringWithAPredicatedObject()
    {
        SimplePredicate predicate = new SimplePredicate(DESCRIPTION, PREDICATED_OBJECT)
        {
            @Override
            public boolean test()
            {
                return true;
            }
        };

        assertThat(predicate.toString(), containsString(DESCRIPTION));
        assertThat(
                "toString should not return the predicated object because " +
                "this is too verbose for the usual toString use cases",
                predicate.toString(),
                not(containsString(PREDICATED_OBJECT)));
    }
}
