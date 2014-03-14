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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class OutputStreamHelper
{
    private ByteArrayOutputStream _byteOutputStream;
    private DataOutputStream _dataOutputStream;

    public OutputStreamHelper()
    {
    }

    public void createOutputStreams()
    {
        _byteOutputStream = new ByteArrayOutputStream();
        _dataOutputStream = new DataOutputStream(_byteOutputStream);
    }

    public void clearOutputStreams()
    {
        _byteOutputStream = null;
        _dataOutputStream = null;
    }

    public boolean hasOutputStreams()
    {
        return _dataOutputStream != null;
    }

    public byte[] getByteOutput()
    {
        return _byteOutputStream.toByteArray();
    }

    /**
     * @see DataOutputStream#writeBoolean(boolean)
     * @throws IOException
     */
    public void writeBoolean(boolean bool) throws IOException
    {
        _dataOutputStream.writeBoolean(bool);
    }

    /**
     * @see DataOutputStream#writeByte(int)
     * @throws IOException
     */
    public void writeByte(int b) throws IOException
    {
        _dataOutputStream.writeByte(b);
    }

    /**
     * @see DataOutputStream#writeShort(int)
     * @throws IOException
     */
    public void writeShort(int s) throws IOException
    {
        _dataOutputStream.writeShort(s);
    }

    /**
     * @see DataOutputStream#writeChar(int)
     * @throws IOException
     */
    public void writeChar(int c) throws IOException
    {
        _dataOutputStream.writeChar(c);
    }

    /**
     * @see DataOutputStream#writeInt(int)
     * @throws IOException
     */
    public void writeInt(int i) throws IOException
    {
        _dataOutputStream.writeInt(i);
    }

    /**
     * @see DataOutputStream#writeLong(long)
     * @throws IOException
     */
    public void writeLong(long l) throws IOException
    {
        _dataOutputStream.writeLong(l);
    }

    /**
     * @see DataOutputStream#writeFloat(float)
     * @throws IOException
     */
    public void writeFloat(float f) throws IOException
    {
        _dataOutputStream.writeFloat(f);
    }

    /**
     * @see DataOutputStream#writeDouble(double)
     * @throws IOException
     */
    public void writeDouble(double d) throws IOException
    {
        _dataOutputStream.writeDouble(d);
    }

    /**
     * @see DataOutputStream#writeUTF(String)
     * @throws IOException
     */
    public void writeUTF(String utf) throws IOException
    {
        _dataOutputStream.writeUTF(utf);
    }

    /**
     * @see DataOutputStream#write(byte[], int, int)
     * @throws IOException
     */
    public void write(byte[] bytes, int offset, int length) throws IOException
    {
        _dataOutputStream.write(bytes, offset, length);
    }
}
