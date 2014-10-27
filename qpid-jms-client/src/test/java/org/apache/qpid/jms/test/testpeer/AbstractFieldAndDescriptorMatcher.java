package org.apache.qpid.jms.test.testpeer;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
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
    protected final Map<Enum<?>, Matcher<?>> fieldMatchers;
    private Map<Enum<?>, Object> receivedFields;

    public AbstractFieldAndDescriptorMatcher(UnsignedLong numericDescriptor, Symbol symbolicDescriptor, Map<Enum<?>, Matcher<?>> fieldMatchers) {
        this.numericDescriptor = numericDescriptor;
        this.symbolicDescriptor = symbolicDescriptor;
        this.fieldMatchers = fieldMatchers;
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

    public Map<Enum<?>, Matcher<?>> getMatchers() {
        return fieldMatchers;
    }

    /**
     * Returns the received values, keyed by enums representing the fields
     * (the enums have an ordinal number matching the AMQP spec field order,
     * and a sensible name)
     */
    protected Map<Enum<?>, Object> getReceivedFields() {
        return receivedFields;
    }

    /**
     * Verifies the fields of the provided list against any matchers registered.
     * @param described the list of fields from the described type.
     * @throws AssertionError if a registered matcher assertion is not met.
     */
    public void verifyFields(List<Object> described) throws AssertionError {
        int fieldNumber = 0;
        HashMap<Enum<?>, Object> valueMap = new HashMap<>();
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