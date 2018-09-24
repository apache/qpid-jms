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
package org.apache.qpid.jms.provider.amqp.message;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;

/**
 * ReadableBuffer implementation that wraps a Netty ByteBuf
 */
public class AmqpReadableBuffer implements ReadableBuffer {

    private ByteBuf buffer;

    public AmqpReadableBuffer(ByteBuf buffer) {
        this.buffer = buffer;
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    @Override
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }

    @Override
    public byte[] array() {
        return buffer.array();
    }

    @Override
    public int arrayOffset() {
        return buffer.arrayOffset();
    }

    @Override
    public ReadableBuffer reclaimRead() {
        return this;
    }

    @Override
    public byte get() {
        return buffer.readByte();
    }

    @Override
    public byte get(int index) {
        return buffer.getByte(index);
    }

    @Override
    public int getInt() {
        return buffer.readInt();
    }

    @Override
    public long getLong() {
        return buffer.readLong();
    }

    @Override
    public short getShort() {
        return buffer.readShort();
    }

    @Override
    public float getFloat() {
        return buffer.readFloat();
    }

    @Override
    public double getDouble() {
        return buffer.readDouble();
    }

    @Override
    public ReadableBuffer get(byte[] target, int offset, int length) {
        buffer.readBytes(target, offset, length);
        return this;
    }

    @Override
    public ReadableBuffer get(byte[] target) {
        buffer.readBytes(target);
        return this;
    }

    @Override
    public ReadableBuffer get(WritableBuffer target) {
        int start = target.position();

        if (buffer.hasArray()) {
            target.put(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), buffer.readableBytes());
        } else {
            target.put(buffer.nioBuffer());
        }

        int written = target.position() - start;

        buffer.readerIndex(buffer.readerIndex() + written);

        return this;
    }

    @Override
    public ReadableBuffer slice() {
        return new AmqpReadableBuffer(buffer.slice());
    }

    @Override
    public ReadableBuffer flip() {
        buffer.setIndex(0, buffer.readerIndex());
        return this;
    }

    @Override
    public ReadableBuffer limit(int limit) {
        buffer.writerIndex(limit);
        return this;
    }

    @Override
    public int limit() {
        return buffer.writerIndex();
    }

    @Override
    public ReadableBuffer position(int position) {
        buffer.readerIndex(position);
        return this;
    }

    @Override
    public int position() {
        return buffer.readerIndex();
    }

    @Override
    public ReadableBuffer mark() {
        buffer.markReaderIndex();
        return this;
    }

    @Override
    public ReadableBuffer reset() {
        buffer.resetReaderIndex();
        return this;
    }

    @Override
    public ReadableBuffer rewind() {
        buffer.readerIndex(0);
        return this;
    }

    @Override
    public ReadableBuffer clear() {
        buffer.setIndex(0, buffer.capacity());
        return this;
    }

    @Override
    public int remaining() {
        return buffer.readableBytes();
    }

    @Override
    public boolean hasRemaining() {
        return buffer.isReadable();
    }

    @Override
    public ReadableBuffer duplicate() {
        return new AmqpReadableBuffer(buffer.duplicate());
    }

    @Override
    public ByteBuffer byteBuffer() {
        return buffer.nioBuffer();
    }

    @Override
    public String readUTF8() throws CharacterCodingException {
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @Override
    public String readString(CharsetDecoder decoder) throws CharacterCodingException {
        return buffer.toString(StandardCharsets.UTF_8);
    }
}
