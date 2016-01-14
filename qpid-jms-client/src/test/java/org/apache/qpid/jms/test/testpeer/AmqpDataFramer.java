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
package org.apache.qpid.jms.test.testpeer;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.codec.Data;

/**
 * Generates frames as per section 2.3.1 of the AMQP spec
 */
public class AmqpDataFramer
{
    private static final int CAPACITY = 2024;
    private static final byte FRAME_PREAMBLE_SIZE_IN_FOUR_BYTE_WORDS = 2;

    public static byte[] encodeFrame(FrameType type, int channel, DescribedType describedType, Binary payload)
    {
        ByteBuffer buffer = ByteBuffer.allocate(CAPACITY);  //TODO: set a proper size

        buffer.position(8); // leave hole for frame header

        if (describedType != null) {
            Data frameBody = Data.Factory.create();
            frameBody.putDescribedType(describedType);
            frameBody.encode(buffer);
        }

        if(payload != null)
        {
            buffer.put(payload.asByteBuffer());
        }

        int frameSize = buffer.position();
        buffer.rewind();
        buffer.putInt(frameSize);
        buffer.put(FRAME_PREAMBLE_SIZE_IN_FOUR_BYTE_WORDS);
        buffer.put((byte)type.ordinal());
        buffer.putShort((short)channel);

        byte[] target = new byte[frameSize];

        buffer.rewind();
        buffer.get(target, 0, frameSize);
        return target;
    }
}
