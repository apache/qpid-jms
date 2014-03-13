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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class InputStreamHelper
{
    private DataInputStream _dataInputStream;

    public InputStreamHelper()
    {
    }

    public void createNewInputStream(ByteArrayInputStream bais)
    {
        _dataInputStream = new DataInputStream(bais);
    }

    public void clearInputStream()
    {
        _dataInputStream = null;
    }

    /**
     * @see DataInputStream#readBoolean()
     * @throws IOException
     */
    public boolean readBoolean() throws IOException
    {
        return _dataInputStream.readBoolean();
    }

    /**
     * @see DataInputStream#readByte()
     * @throws IOException
     */
    public byte readByte() throws IOException
    {
        return _dataInputStream.readByte();
    }

    /**
     * @see DataInputStream#readUnsignedByte()
     * @throws IOException
     */
    public int readUnsignedByte() throws IOException
    {
        return _dataInputStream.readUnsignedByte();
    }

    /**
     * @see DataInputStream#readShort()
     * @throws IOException
     */
    public short readShort() throws IOException
    {
        return _dataInputStream.readShort();
    }

    /**
     * @see DataInputStream#readUnsignedShort()
     * @throws IOException
     */
    public int readUnsignedShort() throws IOException
    {
        return _dataInputStream.readUnsignedShort();
    }

    /**
     * @see DataInputStream#readChar()
     * @throws IOException
     */
    public char readChar() throws IOException
    {
        return _dataInputStream.readChar();
    }

    /**
     * @see DataInputStream#readInt()
     * @throws IOException
     */
    public int readInt() throws IOException
    {
        return _dataInputStream.readInt();
    }

    /**
     * @see DataInputStream#readLong()
     * @throws IOException
     */
    public long readLong() throws IOException
    {
        return _dataInputStream.readLong();
    }

    /**
     * @see DataInputStream#readFloat()
     * @throws IOException
     */
    public float readFloat() throws IOException
    {
        return _dataInputStream.readFloat();
    }

    /**
     * @see DataInputStream#readDouble()
     * @throws IOException
     */
    public double readDouble() throws IOException
    {
        return _dataInputStream.readDouble();
    }

    /**
     * @see DataInputStream#readUTF()
     * @throws IOException
     */
    public String readUTF() throws IOException
    {
        return _dataInputStream.readUTF();
    }

    /**
     * @see DataInputStream#read(byte[], int, int)
     * @throws IOException
     */
    public int read(byte dest[], int offset, int length) throws IOException
    {
        return _dataInputStream.read(dest, offset, length);
    }
}
