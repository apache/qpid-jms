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
package org.apache.qpid.jms.test.testpeer.matchers.sections;


import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.qpid.proton.amqp.Binary;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;

/**
 * Used to verify the Transfer frame payload, i.e the sections of the AMQP message
 * such as the header, properties, and body sections.
 */
public class TransferPayloadCompositeMatcher extends TypeSafeMatcher<Binary>
{
    private MessageHeaderSectionMatcher _msgHeadersMatcher;
    private String _msgHeaderMatcherFailureDescription;

    private MessageAnnotationsSectionMatcher _msgAnnotationsMatcher;
    private String _msgAnnotationsMatcherFailureDescription;
    private MessagePropertiesSectionMatcher _propsMatcher;
    private String _propsMatcherFailureDescription;
    private Matcher<Binary> _msgContentMatcher;
    private String _msgContentMatcherFailureDescription;
    private ApplicationPropertiesSectionMatcher _appPropsMatcher;
    private String _appPropsMatcherFailureDescription;
    private Matcher<Integer> _payloadLengthMatcher;
    private String _payloadLenthMatcherFailureDescription;

    public TransferPayloadCompositeMatcher()
    {
    }

    @Override
    protected boolean matchesSafely(final Binary receivedBinary)
    {
        int origLength = receivedBinary.getLength();
        int bytesConsumed = 0;

        // Length Matcher
        if(_payloadLengthMatcher != null)
        {
            try
            {
                assertThat("Payload length should match", origLength, _payloadLengthMatcher);
            }
            catch(Throwable t)
            {
                _payloadLenthMatcherFailureDescription = "\nPayload Lenfth Matcher generated throwable: " + t;

                return false;
            }
        }

        //MessageHeader Section
        if(_msgHeadersMatcher != null)
        {
            Binary msgHeaderEtcSubBinary = receivedBinary.subBinary(bytesConsumed, origLength - bytesConsumed);
            try
            {
                bytesConsumed += _msgHeadersMatcher.verify(msgHeaderEtcSubBinary);
            }
            catch(Throwable t)
            {
                _msgHeaderMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to MessageHeaderMatcher: " + msgHeaderEtcSubBinary;
                _msgHeaderMatcherFailureDescription += "\nMessageHeaderMatcher generated throwable: " + t;

                return false;
            }
        }

        //MessageAnnotations Section
        if(_msgAnnotationsMatcher != null)
        {
            Binary msgAnnotationsEtcSubBinary = receivedBinary.subBinary(bytesConsumed, origLength - bytesConsumed);
            try
            {
                bytesConsumed += _msgAnnotationsMatcher.verify(msgAnnotationsEtcSubBinary);
            }
            catch(Throwable t)
            {
                _msgAnnotationsMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to MessageAnnotationsMatcher: " + msgAnnotationsEtcSubBinary;
                _msgAnnotationsMatcherFailureDescription += "\nMessageAnnotationsMatcher generated throwable: " + t;

                return false;
            }
        }

        //Properties Section
        if(_propsMatcher != null)
        {
            Binary propsEtcSubBinary = receivedBinary.subBinary(bytesConsumed, origLength - bytesConsumed);
            try
            {
                bytesConsumed += _propsMatcher.verify(propsEtcSubBinary);
            }
            catch(Throwable t)
            {
                _propsMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to PropertiesMatcher: " + propsEtcSubBinary;
                _propsMatcherFailureDescription += "\nPropertiesMatcher generated throwable: " + t;

                return false;
            }
        }

        //Application Properties Section
        if(_appPropsMatcher != null)
        {
            Binary appPropsEtcSubBinary = receivedBinary.subBinary(bytesConsumed, origLength - bytesConsumed);
            try
            {
                bytesConsumed += _appPropsMatcher.verify(appPropsEtcSubBinary);
            }
            catch(Throwable t)
            {
                _appPropsMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to ApplicationPropertiesMatcher: " + appPropsEtcSubBinary;
                _appPropsMatcherFailureDescription += "\nApplicationPropertiesMatcher generated throwable: " + t;

                return false;
            }
        }
        //Message Content Body Section, already a Matcher<Binary>
        if(_msgContentMatcher != null)
        {
            Binary msgContentBodyEtcSubBinary = receivedBinary.subBinary(bytesConsumed, origLength - bytesConsumed);
            boolean contentMatches = _msgContentMatcher.matches(msgContentBodyEtcSubBinary);
            if(!contentMatches)
            {
                Description desc = new StringDescription();
                _msgContentMatcher.describeTo(desc);
                _msgContentMatcher.describeMismatch(msgContentBodyEtcSubBinary, desc);

                _msgContentMatcherFailureDescription = "\nMessageContentMatcher mismatch Description:";
                _msgContentMatcherFailureDescription += desc.toString();

                return false;
            }
        }

        //TODO: we will need figure out a way to determine how many bytes the
        //MessageContentMatcher did/should consume when it comes time to handle footers
        return true;
    }

    @Override
    public void describeTo(Description description)
    {
        description.appendText("a Binary encoding of a Transfer frames payload, containing an AMQP message");
    }

    @Override
    protected void describeMismatchSafely(Binary item, Description mismatchDescription)
    {
        mismatchDescription.appendText("\nActual encoded form of the full Transfer frame payload: ").appendValue(item);

        //Payload Length
        if(_payloadLenthMatcherFailureDescription != null)
        {
            mismatchDescription.appendText("\nPayloadLengthMatcherFailed!");
            mismatchDescription.appendText(_payloadLenthMatcherFailureDescription);
            return;
        }

        //MessageHeaders Section
        if(_msgHeaderMatcherFailureDescription != null)
        {
            mismatchDescription.appendText("\nMessageHeadersMatcherFailed!");
            mismatchDescription.appendText(_msgHeaderMatcherFailureDescription);
            return;
        }

        //MessageAnnotations Section
        if(_msgAnnotationsMatcherFailureDescription != null)
        {
            mismatchDescription.appendText("\nMessageAnnotationsMatcherFailed!");
            mismatchDescription.appendText(_msgAnnotationsMatcherFailureDescription);
            return;
        }

        //Properties Section
        if(_propsMatcherFailureDescription != null)
        {
            mismatchDescription.appendText("\nPropertiesMatcherFailed!");
            mismatchDescription.appendText(_propsMatcherFailureDescription);
            return;
        }

        //Application Properties Section
        if(_appPropsMatcherFailureDescription != null)
        {
            mismatchDescription.appendText("\nApplicationPropertiesMatcherFailed!");
            mismatchDescription.appendText(_appPropsMatcherFailureDescription);
            return;
        }

        //Message Content Body Section
        if(_msgContentMatcherFailureDescription != null)
        {
            mismatchDescription.appendText("\nContentMatcherFailed!");
            mismatchDescription.appendText(_msgContentMatcherFailureDescription);
            return;
        }
    }

    public void setHeadersMatcher(MessageHeaderSectionMatcher msgHeadersMatcher)
    {
        _msgHeadersMatcher = msgHeadersMatcher;
    }

    public void setMessageAnnotationsMatcher(MessageAnnotationsSectionMatcher msgAnnotationsMatcher)
    {
        _msgAnnotationsMatcher = msgAnnotationsMatcher;
    }

    public void setPropertiesMatcher(MessagePropertiesSectionMatcher propsMatcher)
    {
        _propsMatcher = propsMatcher;
    }

    public void setApplicationPropertiesMatcher(ApplicationPropertiesSectionMatcher appPropsMatcher)
    {
        _appPropsMatcher = appPropsMatcher;
    }

    public void setMessageContentMatcher(Matcher<Binary> msgContentMatcher)
    {
        _msgContentMatcher = msgContentMatcher;
    }

    public void setPayloadLengthMatcher(Matcher<Integer> payloadLengthMatcher)
    {
        _payloadLengthMatcher = payloadLengthMatcher;
    }
}