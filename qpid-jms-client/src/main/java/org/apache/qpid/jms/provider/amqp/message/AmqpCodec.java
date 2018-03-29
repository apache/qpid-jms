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

import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_BYTES_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MAP_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_MSG_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_OBJECT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_STREAM_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.JMS_TEXT_MESSAGE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.OCTET_STREAM_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE;
import static org.apache.qpid.jms.provider.amqp.message.AmqpMessageSupport.isContentType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.jms.provider.amqp.AmqpConsumer;
import org.apache.qpid.jms.util.ContentTypeSupport;
import org.apache.qpid.jms.util.InvalidContentTypeException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import io.netty.buffer.ByteBuf;

/**
 * AMQP Codec class used to hide the details of encode / decode
 */
public final class AmqpCodec {

    private static class EncoderDecoderPair {
        DecoderImpl decoder = new DecoderImpl();
        EncoderImpl encoder = new EncoderImpl(decoder);
        {
            AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        }
    }

    private static final ThreadLocal<EncoderDecoderPair> TLS_CODEC = new ThreadLocal<EncoderDecoderPair>() {
        @Override
        protected EncoderDecoderPair initialValue() {
            return new EncoderDecoderPair();
        }
    };

    /**
     * @return a Encoder instance.
     */
    public static EncoderImpl getEncoder() {
        return TLS_CODEC.get().encoder;
    }

    /**
     * @return a Decoder instance.
     */
    public static DecoderImpl getDecoder() {
        return TLS_CODEC.get().decoder;
    }

    /**
     * Given an AMQP Section encode it and return the buffer holding the encoded value
     *
     * @param section
     *      the AMQP Section value to encode.
     *
     * @return a buffer holding the encoded bytes of the given AMQP Section object.
     */
    public static ByteBuf encode(Section section) {
        if (section == null) {
            return null;
        }

        AmqpWritableBuffer buffer = new AmqpWritableBuffer();

        EncoderImpl encoder = getEncoder();
        encoder.setByteBuffer(buffer);
        encoder.writeObject(section);
        encoder.setByteBuffer((WritableBuffer) null);

        return buffer.getBuffer();
    }

    /**
     * Given an encoded AMQP Section, decode the value previously written there.
     *
     * @param encoded
     *      the AMQP Section value to decode.
     *
     * @return a Section object read from its encoded form.
     */
    public static Section decode(ByteBuf encoded) {
        if (encoded == null || !encoded.isReadable()) {
            return null;
        }

        DecoderImpl decoder = TLS_CODEC.get().decoder;
        decoder.setByteBuffer(encoded.nioBuffer());
        Section result = (Section) decoder.readObject();
        decoder.setByteBuffer(null);
        encoded.resetReaderIndex();

        return result;
    }

    /**
     * Given a Message instance, encode the Message to the wire level representation
     * of that Message.
     *
     * @param message
     *      the Message that is to be encoded into the wire level representation.
     *
     * @return a buffer containing the wire level representation of the input Message.
     */
    public static ByteBuf encodeMessage(AmqpJmsMessageFacade message) {
        AmqpWritableBuffer buffer = new AmqpWritableBuffer();

        EncoderImpl encoder = getEncoder();
        encoder.setByteBuffer(buffer);

        Header header = message.getHeader();
        DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        Properties properties = message.getProperties();
        ApplicationProperties applicationProperties = message.getApplicationProperties();
        Section body = message.getBody();
        Footer footer = message.getFooter();

        if (header != null) {
            encoder.writeObject(header);
        }
        if (deliveryAnnotations != null) {
            encoder.writeObject(deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            encoder.writeObject(messageAnnotations);
        }
        if (properties != null) {
            encoder.writeObject(properties);
        }
        if (applicationProperties != null) {
            encoder.writeObject(applicationProperties);
        }
        if (body != null) {
            encoder.writeObject(body);
        }
        if (footer != null) {
            encoder.writeObject(footer);
        }

        encoder.setByteBuffer((WritableBuffer) null);

        return buffer.getBuffer();
    }

    /**
     * Create a new JmsMessage and underlying JmsMessageFacade that represents the proper
     * message type for the incoming AMQP message.
     *
     * @param consumer
     *        The AmqpConsumer instance that will be linked to the decoded message.
     * @param messageBytes
     *        The the raw bytes that compose the incoming message. (Read-Only)
     *
     * @return a AmqpJmsMessageFacade instance decoded from the message bytes.
     *
     * @throws IOException if an error occurs while creating the message objects.
     */
    public static AmqpJmsMessageFacade decodeMessage(AmqpConsumer consumer, ReadableBuffer messageBytes) throws IOException {

        DecoderImpl decoder = getDecoder();
        decoder.setBuffer(messageBytes);

        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        Properties properties = null;
        ApplicationProperties applicationProperties = null;
        Section body = null;
        Footer footer = null;
        Section section = null;

        if (messageBytes.hasRemaining()) {
            section = (Section) decoder.readObject();
        }

        if (section instanceof Header) {
            header = (Header) section;
            if (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();
            } else {
                section = null;
            }

        }
        if (section instanceof DeliveryAnnotations) {
            deliveryAnnotations = (DeliveryAnnotations) section;

            if (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();
            } else {
                section = null;
            }

        }
        if (section instanceof MessageAnnotations) {
            messageAnnotations = (MessageAnnotations) section;

            if (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();
            } else {
                section = null;
            }

        }
        if (section instanceof Properties) {
            properties = (Properties) section;

            if (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();
            } else {
                section = null;
            }

        }
        if (section instanceof ApplicationProperties) {
            applicationProperties = (ApplicationProperties) section;

            if (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();
            } else {
                section = null;
            }

        }
        if (section != null && !(section instanceof Footer)) {
            body = section;

            if (messageBytes.hasRemaining()) {
                section = (Section) decoder.readObject();
            } else {
                section = null;
            }

        }
        if (section instanceof Footer) {
            footer = (Footer) section;
        }

        decoder.setByteBuffer(null);

        // First we try the easy way, if the annotation is there we don't have to work hard.
        AmqpJmsMessageFacade result = createFromMsgAnnotation(messageAnnotations);
        if (result == null) {
            // Next, match specific section structures and content types
            result = createWithoutAnnotation(body, properties);
        }

        if (result != null) {
            result.setHeader(header);
            result.setDeliveryAnnotations(deliveryAnnotations);
            result.setMessageAnnotations(messageAnnotations);
            result.setProperties(properties);
            result.setApplicationProperties(applicationProperties);
            result.setBody(body);
            result.setFooter(footer);
            result.initialize(consumer);

            return result;
        }

        throw new IOException("Could not create a JMS message from incoming message");
    }

    private static AmqpJmsMessageFacade createFromMsgAnnotation(MessageAnnotations messageAnnotations) throws IOException {
        Object annotation = AmqpMessageSupport.getMessageAnnotation(JMS_MSG_TYPE, messageAnnotations);
        if (annotation != null) {
            switch ((byte) annotation) {
                case JMS_MESSAGE:
                    return new AmqpJmsMessageFacade();
                case JMS_BYTES_MESSAGE:
                    return new AmqpJmsBytesMessageFacade();
                case JMS_TEXT_MESSAGE:
                    return new AmqpJmsTextMessageFacade(StandardCharsets.UTF_8);
                case JMS_MAP_MESSAGE:
                    return new AmqpJmsMapMessageFacade();
                case JMS_STREAM_MESSAGE:
                    return new AmqpJmsStreamMessageFacade();
                case JMS_OBJECT_MESSAGE:
                    return new AmqpJmsObjectMessageFacade();
                default:
                    throw new IOException("Invalid JMS Message Type annotation value found in message: " + annotation);
            }
        }

        return null;
    }

    private static AmqpJmsMessageFacade createWithoutAnnotation(Section body, Properties properties) {
        Symbol messageContentType = properties != null ? properties.getContentType() : null;

        if (body == null) {
            if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, messageContentType)) {
                return new AmqpJmsObjectMessageFacade();
            } else if (isContentType(OCTET_STREAM_CONTENT_TYPE, messageContentType) || isContentType(null, messageContentType)) {
                return new AmqpJmsBytesMessageFacade();
            } else {
                Charset charset = getCharsetForTextualContent(messageContentType);
                if (charset != null) {
                    return new AmqpJmsTextMessageFacade(charset);
                } else {
                    return new AmqpJmsMessageFacade();
                }
            }
        } else if (body instanceof Data) {
            if (isContentType(OCTET_STREAM_CONTENT_TYPE, messageContentType) || isContentType(null, messageContentType)) {
                return new AmqpJmsBytesMessageFacade();
            } else if (isContentType(SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, messageContentType)) {
                return new AmqpJmsObjectMessageFacade();
            } else {
                Charset charset = getCharsetForTextualContent(messageContentType);
                if (charset != null) {
                    return new AmqpJmsTextMessageFacade(charset);
                } else {
                    return new AmqpJmsBytesMessageFacade();
                }
            }
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();

            if (value == null || value instanceof String) {
                return new AmqpJmsTextMessageFacade(StandardCharsets.UTF_8);
            } else if (value instanceof Binary) {
                return new AmqpJmsBytesMessageFacade();
            } else {
                return new AmqpJmsObjectMessageFacade();
            }
        } else if (body instanceof AmqpSequence) {
            return new AmqpJmsObjectMessageFacade();
        }

        return null;
    }

    private static Charset getCharsetForTextualContent(Symbol messageContentType) {
        if (messageContentType != null) {
            try {
                return ContentTypeSupport.parseContentTypeForTextualCharset(messageContentType.toString());
            } catch (InvalidContentTypeException e) {
            }
        }

        return null;
    }
}
