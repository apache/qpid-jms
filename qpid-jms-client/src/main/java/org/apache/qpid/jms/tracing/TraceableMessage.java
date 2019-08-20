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
package org.apache.qpid.jms.tracing;

import java.util.function.BiConsumer;

/**
 * Interface which allows generic tracing interactions with Messages
 * from the client.
 */
public interface TraceableMessage {

    /**
     * Gets some trace related context from the message for use by the tracing
     * implementation.
     * <p>
     * Allows the Tracing implementation to store context data into the message
     * without the message needing to know what the types or structure of that
     * data is.
     *
     * @param key
     * 		The name of the context element to be looked up.
     *
     * @return the stored tracing context element or null if not present.
     */
    Object getTracingContext(String key);

    /**
     * Sets some trace related context from the message for use by the tracing
     * implementation.
     * <p>
     * Allows the Tracing implementation to store context data into the message
     * without the message needing to know what the types or structure of that
     * data is.
     *
     * @param key
     * 		The key that the tracing context element should be stored under.
     * @param value
     * 		The value to store under the given key.
     *
     * @return the previous value stored under the given key if any present.
     */
    Object setTracingContext(String key, Object value);

    /**
     * Removes some trace related context from the message.
     *
     * @param key
     *      The key of the tracing context element that should be cleared.
     *
     * @return the previous value stored under the given key if any present.
     */
    Object removeTracingContext(String key);

    /**
     * Gets some trace specific message annotation that was previously applied to
     * the given message either locally or by a remote peer.
     *
     * @param key
     * 		The name of the tracing annotation data to retrieve.
     *
     * @return the tracing related annotation data under the given key.
     */
    Object getTracingAnnotation(String key);

    /**
     * Remove some trace specific message annotation that was previously applied to
     * the given message either locally or by a remote peer.
     *
     * @param key
     * 		The name of the tracing annotation data to remove.
     *
     * @return the tracing related annotation data under the given key.
     */
    Object removeTracingAnnotation(String key);

    /**
     * Sets some trace specific message annotation that was previously applied to
     * the given message either locally or by a remote peer.
     *
     * @param key
     * 		The name of the tracing annotation data to store the trace data under.
     * @param value
     * 		The value to store under the given key.
     *
     * @return the previous value stored under the given key if any present.
     */
    Object setTracingAnnotation(String key, Object value);

    /**
     * Allows the tracing layer to filter out tracing related details from the full
     * set of message annotations that a message might be carrying.
     *
     * @param filter
     * 		The filter used to consume tracing related message annotations of interest.
     */
    void filterTracingAnnotations(BiConsumer<String, Object> filter);
}
