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
 *
 */

package org.apache.qpid.jms.test.testpeer.matchers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import java.util.List;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.jms.test.testpeer.AbstractFieldAndDescriptorMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class ErrorMatcher extends TypeSafeMatcher<Object>
{
    private ErrorMatcherCore coreMatcher = new ErrorMatcherCore();
    private String mismatchTextAddition;
    private Object described;
    private Object descriptor;

    public ErrorMatcher()
    {
    }

    @Override
    protected boolean matchesSafely(Object received)
    {
        try
        {
            assertThat(received, instanceOf(DescribedType.class));
            descriptor = ((DescribedType)received).getDescriptor();
            if(!coreMatcher.descriptorMatches(descriptor))
            {
                mismatchTextAddition = "Descriptor mismatch";
                return false;
            }

            described = ((DescribedType)received).getDescribed();
            assertThat(described, instanceOf(List.class));
            @SuppressWarnings("unchecked")
            List<Object> fields = (List<Object>) described;

            coreMatcher.verifyFields(fields);
        }
        catch (AssertionError ae)
        {
            mismatchTextAddition = "AssertionFailure: " + ae.getMessage();
            return false;
        }

        return true;
    }

    @Override
    protected void describeMismatchSafely(Object item, Description mismatchDescription)
    {
        mismatchDescription.appendText("\nActual form: ").appendValue(item);

        mismatchDescription.appendText("\nExpected descriptor: ")
                .appendValue(coreMatcher.getSymbolicDescriptor())
                .appendText(" / ")
                .appendValue(coreMatcher.getNumericDescriptor());

        if(mismatchTextAddition != null)
        {
            mismatchDescription.appendText("\nAdditional info: ").appendValue(mismatchTextAddition);
        }
    }

    public void describeTo(Description description)
    {
        description
            .appendText("Modified which matches: ")
            .appendValue(coreMatcher.getMatchers());
    }


    public ErrorMatcher withCondition(Matcher<?> m)
    {
        coreMatcher.withCondition(m);
        return this;
    }

    public ErrorMatcher withDescription(Matcher<?> m)
    {
        coreMatcher.withDescription(m);
        return this;
    }

    public ErrorMatcher withInfo(Matcher<?> m)
    {
        coreMatcher.withInfo(m);
        return this;
    }

    public Object getReceivedCondition()
    {
        return coreMatcher.getReceivedCondition();
    }

    public Object getReceivedDescription()
    {
        return coreMatcher.getReceivedDescription();
    }

    public Object getReceivedInfo()
    {
        return coreMatcher.getReceivedInfo();
    }



    //Inner core matching class
    public static class ErrorMatcherCore extends AbstractFieldAndDescriptorMatcher
    {
        /** Note that the ordinals of the Field enums match the order specified in the AMQP spec */
        public enum Field
        {
            CONDITION,
            DESCRIPTION,
            INFO
        }

        public ErrorMatcherCore()
        {
            super(UnsignedLong.valueOf(0x000000000000001DL),
                  Symbol.valueOf("amqp:error:list"));
        }


        public ErrorMatcherCore withCondition(Matcher<?> m)
        {
            getMatchers().put(Field.CONDITION, m);
            return this;
        }

        public ErrorMatcherCore withDescription(Matcher<?> m)
        {
            getMatchers().put(Field.DESCRIPTION, m);
            return this;
        }

        public ErrorMatcherCore withInfo(Matcher<?> m)
        {
            getMatchers().put(Field.INFO, m);
            return this;
        }

        public Object getReceivedCondition()
        {
            return getReceivedFields().get(Field.CONDITION);
        }

        public Object getReceivedDescription()
        {
            return getReceivedFields().get(Field.DESCRIPTION);
        }

        public Object getReceivedInfo()
        {
            return getReceivedFields().get(Field.INFO);
        }

        @Override
        protected Enum<?> getField(int fieldIndex)
        {
            return Field.values()[fieldIndex];
        }
    }
}