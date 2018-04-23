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
 */
package org.apache.qpid.jms.test.testpeer;

import static org.apache.qpid.proton.engine.TransportResultFactory.error;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.codec.Data;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.engine.TransportResult;
import org.apache.qpid.proton.engine.TransportResultFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestFrameParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestFrameParser.class);

    private enum State
    {
        HEADER0,
        HEADER1,
        HEADER2,
        HEADER3,
        HEADER4,
        HEADER5,
        HEADER6,
        HEADER7,
        SIZE_0,
        SIZE_1,
        SIZE_2,
        SIZE_3,
        PRE_PARSE,
        BUFFERING,
        PARSING,
        ERROR
    }


    private State _state = State.HEADER0;

    /** the stated size of the current frame */
    private int _size;

    /** holds the current frame that is being parsed */
    private ByteBuffer _frameBuffer;

    private TestAmqpPeer _peer;

    public TestFrameParser(TestAmqpPeer peer)
    {
        _peer = peer;
    }

    public void expectHeader()
    {
        _state = State.HEADER0;
    }

    public void input(final ByteBuffer input)
    {
        TransportResult frameParsingError = null;
        int size = _size;
        ByteBuffer nextFramesInput = null;
        byte[] header = new byte[8];

        boolean transportAccepting = true;

        ByteBuffer currentInput = input;
        while(currentInput.hasRemaining() && _state != State.ERROR && transportAccepting)
        {
            switch(_state)
            {
                case HEADER0:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[0] = c;
                        _state = State.HEADER1;
                    }
                    else
                    {
                        break;
                    }
                case HEADER1:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[1] = c;
                        _state = State.HEADER2;
                    }
                    else
                    {
                        break;
                    }
                case HEADER2:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[2] = c;
                        _state = State.HEADER3;
                    }
                    else
                    {
                        break;
                    }
                case HEADER3:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[3] = c;
                        _state = State.HEADER4;
                    }
                    else
                    {
                        break;
                    }
                case HEADER4:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[4] = c;
                        _state = State.HEADER5;
                    }
                    else
                    {
                        break;
                    }
                case HEADER5:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[5] = c;
                        _state = State.HEADER6;
                    }
                    else
                    {
                        break;
                    }
                case HEADER6:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[6] = c;
                        _state = State.HEADER7;
                    }
                    else
                    {
                        break;
                    }
                case HEADER7:
                    if(currentInput.hasRemaining())
                    {
                        byte c = currentInput.get();
                        header[7] = c;
                        _peer.receiveHeader(header);
                        _state = State.SIZE_0;
                    }
                    else
                    {
                        break;
                    }
                case SIZE_0:
                    if(!currentInput.hasRemaining())
                    {
                        break;
                    }
                    if(currentInput.remaining() >= 4)
                    {
                        size = currentInput.getInt();
                        _state = State.PRE_PARSE;
                        break;
                    }
                    else
                    {
                        size = (currentInput.get() << 24) & 0xFF000000;
                        if(!currentInput.hasRemaining())
                        {
                            _state = State.SIZE_1;
                            break;
                        }
                    }
                case SIZE_1:
                    size |= (currentInput.get() << 16) & 0xFF0000;
                    if(!currentInput.hasRemaining())
                    {
                        _state = State.SIZE_2;
                        break;
                    }
                case SIZE_2:
                    size |= (currentInput.get() << 8) & 0xFF00;
                    if(!currentInput.hasRemaining())
                    {
                        _state = State.SIZE_3;
                        break;
                    }
                case SIZE_3:
                    size |= currentInput.get() & 0xFF;
                    _state = State.PRE_PARSE;

                case PRE_PARSE:
                    ;
                    if(size < 8)
                    {
                        frameParsingError = error("specified frame size %d smaller than minimum frame header "
                                                         + "size %d",
                                                         _size, 8);
                        _state = State.ERROR;
                        break;
                    }

                    if(currentInput.remaining() < size-4)
                    {
                        _frameBuffer = ByteBuffer.allocate(size-4);
                        _frameBuffer.put(currentInput);
                        _state = State.BUFFERING;
                        break;
                    }
                case BUFFERING:
                    if(_frameBuffer != null)
                    {
                        if(currentInput.remaining() < _frameBuffer.remaining())
                        {
                            // in does not contain enough bytes to complete the frame
                            _frameBuffer.put(currentInput);
                            break;
                        }
                        else
                        {
                            ByteBuffer dup = currentInput.duplicate();
                            dup.limit(dup.position()+_frameBuffer.remaining());
                            currentInput.position(currentInput.position()+_frameBuffer.remaining());
                            _frameBuffer.put(dup);
                            nextFramesInput = currentInput;
                            _frameBuffer.flip();
                            currentInput = _frameBuffer;
                            _state = State.PARSING;
                        }
                    }

                case PARSING:

                    int dataOffset = (currentInput.get() << 2) & 0x3FF;

                    if(dataOffset < 8)
                    {
                        frameParsingError = error("specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8);
                        _state = State.ERROR;
                        break;
                    }
                    else if(dataOffset > size)
                    {
                        frameParsingError = error("specified frame data offset %d larger than the frame size %d", dataOffset, _size);
                        _state = State.ERROR;
                        break;
                    }

                    // type

                    int type = currentInput.get() & 0xFF;
                    int channel = currentInput.getShort() & 0xFFFF;

                    // note that this skips over the extended header if it's present
                    if(dataOffset!=8)
                    {
                        currentInput.position(currentInput.position()+dataOffset-8);
                    }

                    // oldIn null iff not working on duplicated buffer
                    final int frameBodySize = size - dataOffset;
                    if(nextFramesInput == null)
                    {
                        nextFramesInput = currentInput;
                        currentInput = currentInput.duplicate();
                        final int endPos = currentInput.position() + frameBodySize;
                        currentInput.limit(endPos);
                        nextFramesInput.position(endPos);

                    }

                    // in's remaining bytes are now exactly those of one complete frame body
                    // oldIn's remaining bytes are those beyond the end of currentIn's frame

                    try
                    {
                        if (frameBodySize > 0)
                        {

                            Data data = Data.Factory.create();
                            data.decode(currentInput);
                            Data.DataType dataType = data.type();
                            if(dataType != Data.DataType.DESCRIBED)
                            {
                                throw new IllegalArgumentException("Frame body type expected to be " + Data.DataType.DESCRIBED + " but was: " + dataType);
                            }

                            DescribedType describedType = data.getDescribedType();
                            LOGGER.debug("Received described type: {}", describedType);

                            Binary payload;

                            if(currentInput.hasRemaining())
                            {
                                byte[] payloadBytes = new byte[currentInput.remaining()];
                                currentInput.get(payloadBytes);
                                payload = new Binary(payloadBytes);
                            }
                            else
                            {
                                payload = null;
                            }

                            _peer.receiveFrame(type, channel, size, describedType, payload);
                        }
                        else
                        {
                            _peer.receiveEmptyFrame(type, channel);
                        }
                        _size = 0;
                        currentInput = nextFramesInput;
                        nextFramesInput = null;
                        _frameBuffer = null;
                        if(_state != State.HEADER0)
                        {
                            _state = State.SIZE_0;
                        }
                    }
                    catch (DecodeException ex)
                    {
                        _state = State.ERROR;
                        frameParsingError = error(ex);
                    }
                    break;
                case ERROR:
                    // do nothing
            }

        }

        _size = size;

        if(_state == State.ERROR)
        {
            // TODO throw non-proton exception
            if(frameParsingError != null)
            {
                frameParsingError.checkIsOk();
            }
            else
            {
                TransportResultFactory.error("Unable to parse, probably because of a previous error").checkIsOk();
            }
        }
    }
}
