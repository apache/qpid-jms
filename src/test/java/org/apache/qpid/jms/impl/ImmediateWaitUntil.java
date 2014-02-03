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
package org.apache.qpid.jms.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ImmediateWaitUntil implements Answer<Void>
{
    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable
    {
        //verify the arg types / expected values
        Object[] args = invocation.getArguments();
        assertEquals(2, args.length);
        assertTrue(args[0] instanceof Predicate);
        assertTrue(args[1] instanceof Long);

        Predicate predicate = (Predicate)args[0];

        if(predicate.test())
        {
            return null;
        }
        else
        {
            throw new JmsTimeoutException(0, "ImmediateWaitUntil predicate test returned false: " + predicate);
        }
    }

    public static void mockWaitUntil(ConnectionImpl connectionImpl) throws JmsTimeoutException, JmsInterruptedException
    {
        Assert.assertFalse("This method cant mock the method on a real object", connectionImpl.getClass() == ConnectionImpl.class);

        Mockito.doAnswer(new ImmediateWaitUntil()).when(connectionImpl).waitUntil(Matchers.isA(Predicate.class), Matchers.anyLong());
    }
}