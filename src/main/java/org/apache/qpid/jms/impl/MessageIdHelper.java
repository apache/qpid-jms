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

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Helper class for identifying and converting message-id and correlation-id strings.
 *
 * <p>AMQP messages allow for 4 types of message-id/correlation-id: string, binary, uuid, ulong.
 * In order to accept or return a string representation of these for interoperability with
 * other AMQP clients, the following encoding can be used after the "ID:" prefix used for a JMSMessageID<br/>
 *
 * "AMQP_BINARY:&lt;hex representation of binary content&gt;"<br/>
 * "AMQP_UUID:&lt;string representation of uuid&gt;"<br/>
 * "AMQP_ULONG:&lt;string representation of ulong&gt;"<br/>
 * "AMQP_STRING:&lt;string&gt;"<br/>
 *
 * <p>The AMQP_STRING encoding exists only for escaping string values that happen to begin with one
 * of the encoding prefixes (including AMQP_STRING), or for consistency of appearance. It need not be used unless necessary.
 *
 * <p>When setting a JMSMessageID, any value that attempts to identify itself as an encoded Binary, UUID, or ulong but cant be
 * converted into the indicated format will default to being treated as a simple String in its entirety (including the
 * encoding prefix). For example, "AMQP_ULONG:hello" can't be encoded as a ulong, and so the full string "AMQP_ULONG:hello"
 * would be used as the message-id value on the AMQP message.
 *
 * <p>When setting a JMSCorrelationID using setJMSCorrelationID(String id), any value which begins with the "ID:" prefix of a
 * JMSMessageID that attempts to identify itself as an encoded binary, uuid, or ulong but cant be converted into the indicated
 * format will cause an exception to be thrown. Any JMSCorrelationID String being set which does not begin with the "ID:"
 * prefix of a JMSMessageID will beencoded as a String in the AMQP message, regardless whether it includes the above encoding
 * prefixes.
 *
 */
public class MessageIdHelper
{
    public static final String AMQP_STRING_PREFIX="AMQP_STRING:";
    public static final String AMQP_UUID_PREFIX="AMQP_UUID:";
    public static final String AMQP_LONG_PREFIX="AMQP_LONG:";
    public static final String AMQP_BINARY_PREFIX="AMQP_BINARY:";
    public static final String JMS_ID_PREFIX = "ID:";

    private static final int JMS_ID_PREFIX_LENGTH = JMS_ID_PREFIX.length();

    /**
     * Checks whether the given string begins with "ID:" prefix used to denote a JMSMessageID
     *
     * @param string the string to check
     * @return true if and only id the string begins with "ID:"
     */
    public boolean hasMessageIdPrefix(String string)
    {
        if(string == null)
        {
            return false;
        }

        return string.startsWith(JMS_ID_PREFIX);
    }

    /**
     * Returns the suffix of the given string after removing the first "ID:" prefix (if present).
     *
     * @param string the string to process
     * @return the suffix, or the original String if the "ID:" prefix is not present
     */
    public String stripMessageIdPrefix(String id)
    {
        if(hasMessageIdPrefix(id))
        {
            return id.substring(JMS_ID_PREFIX_LENGTH);
        }
        else
        {
            return id;
        }
    }

    public String toBaseMessageIdString(Object messageId)
    {
        if(messageId == null)
        {
            return null;
        }
        else if(messageId instanceof String)
        {
            String stringId = (String) messageId;

            //If the given string has a type encoding prefix,
            //we need to escape it as an encoded string (even if
            //the existing encoding prefix was also for string)
            if(hasTypeEncodingPrefix(stringId))
            {
                return AMQP_STRING_PREFIX + stringId;
            }
            else
            {
                return stringId;
            }
        }
        else if(messageId instanceof UUID)
        {
            return AMQP_UUID_PREFIX + messageId.toString();
        }
        else if(messageId instanceof Number)
        {
            //TODO: use Byte/Short/Integer/Long/BigInteger check instead?
            return AMQP_LONG_PREFIX + messageId.toString();
        }
        else if(messageId instanceof ByteBuffer)
        {
            //TODO: implement
            throw new UnsupportedOperationException("Support for Binary has yet to be implemented");
        }
        else
        {
            throw new IllegalArgumentException("Unsupported type provided: " + messageId.getClass());
        }
    }

    private boolean hasTypeEncodingPrefix(String stringId)
    {
        return stringId.startsWith(AMQP_BINARY_PREFIX) ||
                    stringId.startsWith(AMQP_UUID_PREFIX) ||
                        stringId.startsWith(AMQP_LONG_PREFIX) ||
                            stringId.startsWith(AMQP_STRING_PREFIX);
    }
}
