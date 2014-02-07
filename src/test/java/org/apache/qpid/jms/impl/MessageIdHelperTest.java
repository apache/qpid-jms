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

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.UUID;

import org.apache.qpid.jms.QpidJmsTestCase;
import org.junit.Before;
import org.junit.Test;

public class MessageIdHelperTest extends QpidJmsTestCase
{
    private MessageIdHelper _messageIdHelper;

    @Before
    public void setUp() throws Exception
    {
        super.setUp();

        _messageIdHelper = new MessageIdHelper();
    }

    /**
     * Test that {@link MessageIdHelper#hasMessageIdPrefix(String)} returns true for strings that begin "ID:"
     */
    @Test
    public void testHasIdPrefixWithPrefix()
    {
        String myId = "ID:something";
        assertTrue("'ID:' prefix should have been identified", _messageIdHelper.hasMessageIdPrefix(myId));
    }

    /**
     * Test that {@link MessageIdHelper#hasMessageIdPrefix(String)} returns false for string beings "ID" without colon.
     */
    @Test
    public void testHasIdPrefixWithIDButNoColonPrefix()
    {
        String myIdNoColon = "IDsomething";
        assertFalse("'ID' prefix should not have been identified without trailing colon", _messageIdHelper.hasMessageIdPrefix(myIdNoColon));
    }

    /**
     * Test that {@link MessageIdHelper#hasMessageIdPrefix(String)} returns false for null
     */
    @Test
    public void testHasIdPrefixWithNull()
    {
        String nullString = null;
        assertFalse("null string should not result in identification as having the prefix", _messageIdHelper.hasMessageIdPrefix(nullString));
    }

    /**
     * Test that {@link MessageIdHelper#hasMessageIdPrefix(String)} returns false for strings that doesnt have "ID:" anywhere
     */
    @Test
    public void testHasIdPrefixWithoutPrefix()
    {
        String myNonId = "something";
        assertFalse("string without 'ID:' anywhere should not have been identified as having the prefix", _messageIdHelper.hasMessageIdPrefix(myNonId));
    }

    /**
     * Test that {@link MessageIdHelper#hasMessageIdPrefix(String)} returns false for strings has lowercase "id:" prefix
     */
    @Test
    public void testHasIdPrefixWithLowercaseID()
    {
        String myLowerCaseNonId = "id:something";
        assertFalse("lowercase 'id:' prefix should not result in identification as having 'ID:' prefix", _messageIdHelper.hasMessageIdPrefix(myLowerCaseNonId));
    }

    /**
     * Test that {@link MessageIdHelper#stripMessageIdPrefix(String)} strips "ID:" from strings that do begin "ID:"
     */
    @Test
    public void testStripMessageIdPrefixWithPrefix()
    {
        String myIdWithoutPrefix = "something";
        String myId = "ID:" + myIdWithoutPrefix;
        assertEquals("'ID:' prefix should have been stripped", myIdWithoutPrefix, _messageIdHelper.stripMessageIdPrefix(myId));
    }

    /**
     * Test that {@link MessageIdHelper#stripMessageIdPrefix(String)} only strips one "ID:" from strings that
     * begin "ID:ID:...."
     */
    @Test
    public void testStripMessageIdPrefixWithDoublePrefix()
    {
        String myIdWithSinglePrefix = "ID:something";
        String myIdWithDoublePrefix = "ID:" + myIdWithSinglePrefix;
        assertEquals("'ID:' prefix should only have been stripped once", myIdWithSinglePrefix, _messageIdHelper.stripMessageIdPrefix(myIdWithDoublePrefix));
    }

    /**
     * Test that {@link MessageIdHelper#stripMessageIdPrefix(String)} does not alter strings that begins "ID" without a colon.
     */
    @Test
    public void testStripMessageIdPrefixWithIDButNoColonPrefix()
    {
        String myIdNoColon = "IDsomething";
        assertEquals("string without 'ID:' prefix should have been returned unchanged", myIdNoColon, _messageIdHelper.stripMessageIdPrefix(myIdNoColon));
    }

    /**
     * Test that {@link MessageIdHelper#stripMessageIdPrefix(String)} returns null if given null;
     */
    @Test
    public void testStripMessageIdPrefixWithNull()
    {
        String nullString = null;
        assertNull("null string should have been returned", _messageIdHelper.stripMessageIdPrefix(nullString));
    }

    /**
     * Test that {@link MessageIdHelper#stripMessageIdPrefix(String)} does not alter string that doesn't begin "ID:"
     */
    @Test
    public void testStripMessageIdPrefixWithoutIDAnywhere()
    {
        String myNonId = "something";
        assertEquals("string without 'ID:' anywhere should have been returned unchanged", myNonId, _messageIdHelper.stripMessageIdPrefix(myNonId));
    }

    /**
     * Test that {@link MessageIdHelper#stripMessageIdPrefix(String)} does not alter string with lowercase "id:"
     */
    @Test
    public void testStripMessageIdPrefixWithLowercaseID()
    {
        String myLowerCaseNonId = "id:something";
        assertEquals("string with lowercase 'id:' prefix should have been returned unchanged", myLowerCaseNonId, _messageIdHelper.stripMessageIdPrefix(myLowerCaseNonId));
    }

    //TODO: delete this marker comment
    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns null if given null
     */
    @Test
    public void testToBaseMessageIdStringWithNull()
    {
        String nullString = null;
        assertNull("null string should have been returned", _messageIdHelper.toBaseMessageIdString(nullString));
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} throws an IAE if given an unexpected object type.
     */
    @Test
    public void testToBaseMessageIdStringThrowsIAEWithUnexpectedType()
    {
        try
        {
            _messageIdHelper.toBaseMessageIdString(new Object());
            fail("expected exception not thrown");
        }
        catch(IllegalArgumentException iae)
        {
            //expected
        }
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns the given
     * basic string unchanged
     */
    @Test
    public void testToBaseMessageIdStringWithString()
    {
        String stringMessageId = "myIdString";

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(stringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", stringMessageId, baseMessageIdString);
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link MessageIdHelper#AMQP_UUID_PREFIX}.
     */
    @Test
    public void testToBaseMessageIdStringWithStringBeginningWithEncodingPrefixForUUID()
    {
        String uuidStringMessageId = MessageIdHelper.AMQP_UUID_PREFIX + UUID.randomUUID();
        String expected = MessageIdHelper.AMQP_STRING_PREFIX + uuidStringMessageId;

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(uuidStringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns a string
     * indicating an AMQP encoded string, when the given string happens to already begin with
     * the {@link MessageIdHelper#AMQP_ULONG_PREFIX}.
     */
    @Test
    public void testToBaseMessageIdStringWithStringBeginningWithEncodingPrefixForLong()
    {
        String longStringMessageId = MessageIdHelper.AMQP_ULONG_PREFIX + Long.valueOf(123456789L);
        String expected = MessageIdHelper.AMQP_STRING_PREFIX + longStringMessageId;

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(longStringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns a string
     * indicating an AMQP encoded string (effectively twice), when the given string happens to already begin with
     * the {@link MessageIdHelper#AMQP_STRING_PREFIX}.
     */
    @Test
    public void testToBaseMessageIdStringWithStringBeginningWithEncodingPrefixForString()
    {
        String stringMessageId = MessageIdHelper.AMQP_STRING_PREFIX + "myStringId";
        String expected = MessageIdHelper.AMQP_STRING_PREFIX + stringMessageId;

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(stringMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns a string
     * indicating an AMQP encoded UUID when given a UUID object.
     */
    @Test
    public void testToBaseMessageIdStringWithUUID()
    {
        UUID uuidMessageId = UUID.randomUUID();
        String expected = MessageIdHelper.AMQP_UUID_PREFIX + uuidMessageId.toString();

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(uuidMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns a string
     * indicating an AMQP encoded Long when given a Long object.
     */
    @Test
    public void testToBaseMessageIdStringWithUlong()
    {
        Long longMessageId = Long.valueOf(123456789L);
        String expected = MessageIdHelper.AMQP_ULONG_PREFIX + longMessageId.toString();

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(longMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    /**
     * Test that {@link MessageIdHelper#toBaseMessageIdString(String)} returns a string
     * indicating an AMQP encoded Long when given a BigInteger object.
     */
    @Test
    public void testToBaseMessageIdStringWithBigInteger()
    {
        BigInteger bigIntMessageId = BigInteger.valueOf(123456789L);
        String expected = MessageIdHelper.AMQP_ULONG_PREFIX + bigIntMessageId.toString();

        String baseMessageIdString = _messageIdHelper.toBaseMessageIdString(bigIntMessageId);
        assertNotNull("null string should not have been returned", baseMessageIdString);
        assertEquals("expected base id string was not returned", expected, baseMessageIdString);
    }

    //TODO: delete this marker comment
    /**
     * Test that {@link MessageIdHelper#toIdObject(String)} returns a ulong
     * (represented as a BigInteger) when given a string indicating an
     * encoded AMQP ulong id.
     */
    @Test
    public void testToIdObjectWithEncodedUlong()
    {
        BigInteger longId = BigInteger.valueOf(123456789L);
        String provided = MessageIdHelper.AMQP_ULONG_PREFIX + "123456789";

        Object idObject = _messageIdHelper.toIdObject(provided);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", longId, idObject);
    }

    /**
     * Test that {@link MessageIdHelper#toIdObject(String)} returns a UUID
     * when given a string indicating an encoded AMQP uuid id.
     */
    @Test
    public void testToIdObjectWithEncodedUuid()
    {
        UUID uuid = UUID.randomUUID();
        String provided = MessageIdHelper.AMQP_UUID_PREFIX + uuid.toString();

        Object idObject = _messageIdHelper.toIdObject(provided);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", uuid, idObject);
    }

    /**
     * Test that {@link MessageIdHelper#toIdObject(String)} returns a string
     * when given a string without any type encoding prefix.
     */
    @Test
    public void testToIdObjectWithStringContainingNoEncodingPrefix()
    {
        String stringId = "myStringId";

        Object idObject = _messageIdHelper.toIdObject(stringId);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", stringId, idObject);
    }

    /**
     * Test that {@link MessageIdHelper#toIdObject(String)} returns the remainder of the
     * provided string after removing the {@link MessageIdHelper#AMQP_STRING_PREFIX} prefix.
     */
    @Test
    public void testToIdObjectWithStringContainingStringEncodingPrefix()
    {
        String suffix = "myStringSuffix";
        String stringId = MessageIdHelper.AMQP_STRING_PREFIX + suffix;

        Object idObject = _messageIdHelper.toIdObject(stringId);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", suffix, idObject);
    }

    /**
     * Test that when given a string with with the {@link MessageIdHelper#AMQP_STRING_PREFIX} prefix
     * and then additionally the {@link MessageIdHelper#AMQP_UUID_PREFIX}, the
     * {@link MessageIdHelper#toIdObject(String)} method returns the remainder of the provided string
     *  after removing the {@link MessageIdHelper#AMQP_STRING_PREFIX} prefix.
     */
    @Test
    public void testToIdObjectWithStringContainingStringEncodingPrefixAndThenUuidPrefix()
    {
        String encodedUuidString = MessageIdHelper.AMQP_UUID_PREFIX + UUID.randomUUID().toString();
        String stringId = MessageIdHelper.AMQP_STRING_PREFIX + encodedUuidString;

        Object idObject = _messageIdHelper.toIdObject(stringId);
        assertNotNull("null object should not have been returned", idObject);
        assertEquals("expected id object was not returned", encodedUuidString, idObject);
    }
}
