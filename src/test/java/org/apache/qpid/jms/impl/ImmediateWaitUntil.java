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