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
package org.apache.qpid.jms.selector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;

import org.apache.qpid.jms.selector.filter.BooleanExpression;
import org.apache.qpid.jms.selector.filter.ComparisonExpression;
import org.apache.qpid.jms.selector.filter.Expression;
import org.apache.qpid.jms.selector.filter.FilterException;
import org.apache.qpid.jms.selector.filter.Filterable;
import org.apache.qpid.jms.selector.filter.LogicExpression;
import org.apache.qpid.jms.selector.filter.PropertyExpression;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectorParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(SelectorParserTest.class);

    @Test
    public void testParseWithParensAround() throws Exception {
        String[] values = {"x = 1 and y = 2", "(x = 1) and (y = 2)", "((x = 1) and (y = 2))"};

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            LOG.info("Parsing: " + value);

            BooleanExpression andExpression = parse(value);
            assertTrue(andExpression instanceof LogicExpression, "Created LogicExpression expression");
            LogicExpression logicExpression = (LogicExpression)andExpression;
            Expression left = logicExpression.getLeft();
            Expression right = logicExpression.getRight();

            assertTrue(left instanceof ComparisonExpression, "Left is a binary filter");
            assertTrue(right instanceof ComparisonExpression, "Right is a binary filter");
            ComparisonExpression leftCompare = (ComparisonExpression)left;
            ComparisonExpression rightCompare = (ComparisonExpression)right;
            assertPropertyExpression("left", leftCompare.getLeft(), "x");
            assertPropertyExpression("right", rightCompare.getLeft(), "y");
        }
    }

    protected void assertPropertyExpression(String message, Expression expression, String expected) {
        assertTrue(expression instanceof PropertyExpression, message + ". Must be PropertyExpression");
        PropertyExpression propExp = (PropertyExpression)expression;
        assertEquals(expected, propExp.getName(), message + ". Property name");
    }

    protected BooleanExpression parse(String text) throws Exception {
        return SelectorParser.parse(text);
    }

    @Test
    public void testBooleanSelector() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "(trueProp OR falseProp) AND trueProp", true);
        assertSelector(message, "(trueProp OR falseProp) AND falseProp", false);
    }

    @Test
    public void testJMSPropertySelectors() throws Exception {
        MockMessage message = createMessage();
        message.setJMSType("selector-test");
        message.setJMSMessageID("id:test:1:1:1:1");

        assertSelector(message, "JMSType = 'selector-test'", true);
        assertSelector(message, "JMSType = 'crap'", false);

        assertSelector(message, "JMSMessageID = 'id:test:1:1:1:1'", true);
        assertSelector(message, "JMSMessageID = 'id:not-test:1:1:1:1'", false);

        message = createMessage();
        message.setJMSType("1001");

        assertSelector(message, "JMSType='1001'", true);
        assertSelector(message, "JMSType='1001' OR JMSType='1002'", true);
        assertSelector(message, "JMSType = 'crap'", false);
    }

    @Test
    public void testBasicSelectors() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "name = 'James'", true);
        assertSelector(message, "rank > 100", true);
        assertSelector(message, "rank >= 123", true);
        assertSelector(message, "rank >= 124", false);
    }

    @Test
    public void testPropertyTypes() throws Exception {
        MockMessage message = createMessage();
        assertSelector(message, "byteProp = 123", true);
        assertSelector(message, "byteProp = 10", false);
        assertSelector(message, "byteProp2 = 33", true);
        assertSelector(message, "byteProp2 = 10", false);

        assertSelector(message, "shortProp = 123", true);
        assertSelector(message, "shortProp = 10", false);

        assertSelector(message, "shortProp = 123", true);
        assertSelector(message, "shortProp = 10", false);

        assertSelector(message, "intProp = 123", true);
        assertSelector(message, "intProp = 10", false);

        assertSelector(message, "longProp = 123", true);
        assertSelector(message, "longProp = 10", false);

        assertSelector(message, "floatProp = 123", true);
        assertSelector(message, "floatProp = 10", false);

        assertSelector(message, "doubleProp = 123", true);
        assertSelector(message, "doubleProp = 10", false);
    }

    @Test
    public void testAndSelectors() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "name = 'James' and rank < 200", true);
        assertSelector(message, "name = 'James' and rank > 200", false);
        assertSelector(message, "name = 'Foo' and rank < 200", false);
        assertSelector(message, "unknown = 'Foo' and anotherUnknown < 200", false);
    }

    @Test
    public void testOrSelectors() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "name = 'James' or rank < 200", true);
        assertSelector(message, "name = 'James' or rank > 200", true);
        assertSelector(message, "name = 'Foo' or rank < 200", true);
        assertSelector(message, "name = 'Foo' or rank > 200", false);
        assertSelector(message, "unknown = 'Foo' or anotherUnknown < 200", false);
    }

    @Test
    public void testPlus() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "rank + 2 = 125", true);
        assertSelector(message, "(rank + 2) = 125", true);
        assertSelector(message, "125 = (rank + 2)", true);
        assertSelector(message, "rank + version = 125", true);
        assertSelector(message, "rank + 2 < 124", false);
        assertSelector(message, "name + '!' = 'James!'", true);
    }

    @Test
    public void testMinus() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "rank - 2 = 121", true);
        assertSelector(message, "rank - version = 121", true);
        assertSelector(message, "rank - 2 > 122", false);
    }

    @Test
    public void testMultiply() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "rank * 2 = 246", true);
        assertSelector(message, "rank * version = 246", true);
        assertSelector(message, "rank * 2 < 130", false);
    }

    @Test
    public void testDivide() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "rank / version = 61.5", true);
        assertSelector(message, "rank / 3 > 100.0", false);
        assertSelector(message, "rank / 3 > 100", false);
        assertSelector(message, "version / 2 = 1", true);
    }

    @Test
    public void testBetween() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "rank between 100 and 150", true);
        assertSelector(message, "rank between 10 and 120", false);
    }

    @Test
    public void testIn() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "name in ('James', 'Bob', 'Gromit')", true);
        assertSelector(message, "name in ('Bob', 'James', 'Gromit')", true);
        assertSelector(message, "name in ('Gromit', 'Bob', 'James')", true);

        assertSelector(message, "name in ('Gromit', 'Bob', 'Cheddar')", false);
        assertSelector(message, "name not in ('Gromit', 'Bob', 'Cheddar')", true);
    }

    @Test
    public void testIsNull() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "dummy is null", true);
        assertSelector(message, "dummy is not null", false);
        assertSelector(message, "name is not null", true);
        assertSelector(message, "name is null", false);
    }

    @Test
    public void testLike() throws Exception {
        MockMessage message = createMessage();
        message.setStringProperty("modelClassId", "com.whatever.something.foo.bar");
        message.setStringProperty("modelInstanceId", "170");
        message.setStringProperty("modelRequestError", "abc");
        message.setStringProperty("modelCorrelatedClientId", "whatever");

        assertSelector(message, "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')",
                       true);

        message.setStringProperty("modelCorrelatedClientId", "shouldFailNow");

        assertSelector(message, "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')",
                       false);

        message = createMessage();
        message.setStringProperty("modelClassId", "com.whatever.something.foo.bar");
        message.setStringProperty("modelInstanceId", "170");
        message.setStringProperty("modelCorrelatedClientId", "shouldNotMatch");

        assertSelector(message, "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')",
                       true);
    }

    /**
     * Test cases from Mats Henricson
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testMatsHenricsonUseCases() throws Exception {
        MockMessage message = createMessage();
        assertSelector(message, "SessionserverId=1870414179", false);

        message.setLongProperty("SessionserverId", 1870414179);
        assertSelector(message, "SessionserverId=1870414179", true);

        message.setLongProperty("SessionserverId", 1234);
        assertSelector(message, "SessionserverId=1870414179", false);

        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", false);

        message.setStringProperty("Command", "Cheese");
        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", true);

        message.setStringProperty("Command", "MirrorLobbyRequest");
        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", false);
        message.setStringProperty("Command", "MirrorLobbyReply");
        assertSelector(message, "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')", false);
    }

    @SuppressWarnings("unused")
    @Test
    public void testFloatComparisons() throws Exception {
        MockMessage message = createMessage();

        // JMS 1.1 Section 3.8.1.1 : Approximate literals use the Java
        // floating-point literal syntax.
        // We will use the java variable x to demo valid floating point syntax.
        double x;

        // test decimals like x.x
        x = 1.0;
        x = -1.1;
        x = 1.0E1;
        x = 1.1E1;
        x = -1.1E1;
        assertSelector(message, "1.0 < 1.1", true);
        assertSelector(message, "-1.1 < 1.0", true);
        assertSelector(message, "1.0E1 < 1.1E1", true);
        assertSelector(message, "-1.1E1 < 1.0E1", true);

        // test decimals like x.
        x = 1.;
        x = 1.E1;
        assertSelector(message, "1. < 1.1", true);
        assertSelector(message, "-1.1 < 1.", true);
        assertSelector(message, "1.E1 < 1.1E1", true);
        assertSelector(message, "-1.1E1 < 1.E1", true);

        // test decimals like .x
        x = .5;
        x = -.5;
        x = .5E1;
        assertSelector(message, ".1 < .5", true);
        assertSelector(message, "-.5 < .1", true);
        assertSelector(message, ".1E1 < .5E1", true);
        assertSelector(message, "-.5E1 < .1E1", true);

        // test exponents
        x = 4E10;
        x = -4E10;
        x = 5E+10;
        x = 5E-10;
        assertSelector(message, "4E10 < 5E10", true);
        assertSelector(message, "5E8 < 5E10", true);
        assertSelector(message, "-4E10 < 2E10", true);
        assertSelector(message, "-5E8 < 2E2", true);
        assertSelector(message, "4E+10 < 5E+10", true);
        assertSelector(message, "4E-10 < 5E-10", true);
    }

    @Test
    public void testStringQuoteParsing() throws Exception {
        MockMessage message = createMessage();
        assertSelector(message, "quote = '''In God We Trust'''", true);
    }

    @Test
    public void testLikeComparisons() throws Exception {
        MockMessage message = createMessage();

        assertSelector(message, "quote LIKE '''In G_d We Trust'''", true);
        assertSelector(message, "quote LIKE '''In Gd_ We Trust'''", false);
        assertSelector(message, "quote NOT LIKE '''In G_d We Trust'''", false);
        assertSelector(message, "quote NOT LIKE '''In Gd_ We Trust'''", true);

        assertSelector(message, "foo LIKE '%oo'", true);
        assertSelector(message, "foo LIKE '%ar'", false);
        assertSelector(message, "foo NOT LIKE '%oo'", false);
        assertSelector(message, "foo NOT LIKE '%ar'", true);

        assertSelector(message, "foo LIKE '!_%' ESCAPE '!'", true);
        assertSelector(message, "quote LIKE '!_%' ESCAPE '!'", false);
        assertSelector(message, "foo NOT LIKE '!_%' ESCAPE '!'", false);
        assertSelector(message, "quote NOT LIKE '!_%' ESCAPE '!'", true);

        assertSelector(message, "punctuation LIKE '!#$&()*+,-./:;<=>?@[\\]^`{|}~'", true);
    }

    @Test
    public void testInvalidSelector() throws Exception {
        MockMessage message = createMessage();
        assertInvalidSelector(message, "3+5");
        assertInvalidSelector(message, "True AND 3+5");
        assertInvalidSelector(message, "=TEST 'test'");
        assertInvalidSelector(message, "prop1 = prop2 foo AND string = 'Test'");
        assertInvalidSelector(message, "a = 1 AMD  b = 2");
    }

    @Test
    public void testHyphenatedProperty() throws Exception {
        MockMessage message = createMessage();
        message.setStringProperty("hyphenated-prop", "val");

        assertSelector(message, "\"hyphenated-prop\" = 'val'", true);
        assertSelector(message, "\"hyphenated-prop\" = 'other'", false);
    }

    protected MockMessage createMessage() {
        MockMessage message = createMessage("FOO.BAR");
        message.setJMSType("selector-test");
        message.setJMSMessageID("connection:1:1:1:1");
        message.setObjectProperty("name", "James");
        message.setObjectProperty("location", "London");

        message.setByteProperty("byteProp", (byte)123);
        message.setByteProperty("byteProp2", (byte)33);
        message.setShortProperty("shortProp", (short)123);
        message.setIntProperty("intProp", 123);
        message.setLongProperty("longProp", 123);
        message.setFloatProperty("floatProp", 123);
        message.setDoubleProperty("doubleProp", 123);

        message.setIntProperty("rank", 123);
        message.setIntProperty("version", 2);
        message.setStringProperty("quote", "'In God We Trust'");
        message.setStringProperty("foo", "_foo");
        message.setStringProperty("punctuation", "!#$&()*+,-./:;<=>?@[\\]^`{|}~");
        message.setBooleanProperty("trueProp", true);
        message.setBooleanProperty("falseProp", false);
        return message;
    }

    protected void assertInvalidSelector(MockMessage message, String text) {
        try {
            SelectorParser.parse(text);
            fail("Created a valid selector");
        } catch (FilterException e) {
        }
    }

    protected void assertSelector(MockMessage message, String text, boolean expected) throws FilterException {
        BooleanExpression selector = SelectorParser.parse(text);
        assertTrue(selector != null, "Created a valid selector");
        boolean value = selector.matches(message);
        assertEquals(expected, value, "Selector for: " + text);
    }

    protected MockMessage createMessage(String subject) {
        MockMessage message = new MockMessage();
        message.setDestination(subject);
        return message;
    }

    static class MockMessage implements Filterable {

        HashMap<String, Object> properties = new HashMap<String, Object>();
        private String text;
        private Object destination;
        private String messageId;
        private String type;
        private Object localConnectionId;

        public void setDestination(Object destination) {
            this.destination=destination;
        }
        public void setJMSMessageID(String messageId) {
            this.messageId = messageId;
        }
        public void setJMSType(String type) {
            this.type = type;
        }
        public void setText(String text) {
            this.text = text;
        }

        public void setBooleanProperty(String key, boolean value) {
            properties.put(key, value);
        }

        public void setStringProperty(String key, String value) {
            properties.put(key, value);
        }

        public void setByteProperty(String key, byte value) {
            properties.put(key, value);
        }

        public void setDoubleProperty(String key, double value) {
            properties.put(key, value);
        }

        public void setFloatProperty(String key, float value) {
            properties.put(key, value);
        }

        public void setLongProperty(String key, long value) {
            properties.put(key, value);
        }

        public void setIntProperty(String key, int value) {
            properties.put(key, value);
        }

        public void setShortProperty(String key, short value) {
            properties.put(key, value);
        }

        public void setObjectProperty(String key, Object value) {
            properties.put(key, value);
        }

        @Override
        public <T> T getBodyAs(Class<T> type) throws FilterException {
            if( type == String.class ) {
                return type.cast(text);
            }
            return null;
        }

        @Override
        public Object getProperty(String name) {
            if( "JMSType".equals(name) ) {
                return type;
            }
            if( "JMSMessageID".equals(name) ) {
                return messageId;
            }
            return properties.get(name);
        }

        @SuppressWarnings("unchecked")
        public <T> T getDestination() {
            return (T)destination;
        }

        @Override
        public Object getLocalConnectionId() {
            return localConnectionId;
        }
    }
}
