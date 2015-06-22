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
package org.apache.qpid.jms.test.testpeer;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFieldAndDescriptorMatcher {

    protected final Logger _logger = LoggerFactory.getLogger(getClass());

    private final UnsignedLong numericDescriptor;
    private final Symbol symbolicDescriptor;
    protected final Map<Enum<?>, Matcher<?>> fieldMatchers = new LinkedHashMap<>();
    private Map<Enum<?>, Object> receivedFields;

    public AbstractFieldAndDescriptorMatcher(UnsignedLong numericDescriptor, Symbol symbolicDescriptor) {
        this.numericDescriptor = numericDescriptor;
        this.symbolicDescriptor = symbolicDescriptor;
    }

    public UnsignedLong getNumericDescriptor() {
        return numericDescriptor;
    }

    public Symbol getSymbolicDescriptor() {
        return symbolicDescriptor;
    }

    public boolean descriptorMatches(Object descriptor) {
        return numericDescriptor.equals(descriptor) || symbolicDescriptor.equals(descriptor);
    }

    /**
     * A map of field matchers, keyed by enums representing the field to match against.
     *
     * The enums need to have an ordinal number matching their field order position within
     * the frame in the AMQP spec, and should be named according to the spec.
     *
     * @return the map of matchers
     */
    public Map<Enum<?>, Matcher<?>> getMatchers() {
        return fieldMatchers;
    }

    /**
     * Returns the received values, keyed by enums representing the field.
     *
     * The enums have an ordinal number matching their field order position within
     * the frame in the AMQP spec, and are named according to the spec.
     *
     * @return the map of received values
     */
    protected Map<Enum<?>, Object> getReceivedFields() {
        return receivedFields;
    }

    /**
     * Verifies the fields of the provided list against any matchers registered.
     * @param described the list of fields from the described type.
     * @throws AssertionError if a registered matcher assertion is not met.
     */
    public final void verifyFields(List<Object> described) throws AssertionError {
        int fieldNumber = 0;
        HashMap<Enum<?>, Object> valueMap = new LinkedHashMap<>();
        for (Object value : described) {
            valueMap.put(getField(fieldNumber++), value);
        }

        receivedFields = valueMap;

        _logger.debug("About to check the fields of the described type." + "\n  Received:" + valueMap + "\n  Expectations: " + fieldMatchers);
        for (Map.Entry<Enum<?>, Matcher<?>> entry : fieldMatchers.entrySet()) {
            @SuppressWarnings("unchecked")
            Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
            Enum<?> field = entry.getKey();
            assertThat("Field " + field + " value should match", valueMap.get(field), matcher);
        }
    }

    /**
     * Intended to be overridden in most cases (but not necessarily all - hence not marked as abstract)
     */
    protected Enum<?> getField(int fieldIndex) {
        throw new UnsupportedOperationException("getField is expected to be overridden by subclass if it is required");
    }
}