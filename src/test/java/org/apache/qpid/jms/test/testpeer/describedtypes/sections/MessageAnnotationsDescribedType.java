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
package org.apache.qpid.jms.test.testpeer.describedtypes.sections;

import org.apache.qpid.jms.test.testpeer.MapDescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

public class MessageAnnotationsDescribedType extends MapDescribedType
{
    private static final Symbol DESCIPTOR_SYMBOL = Symbol.valueOf("amqp:message-annotations:map");

    @Override
    public Object getDescriptor()
    {
        return DESCIPTOR_SYMBOL;
    }

    public void setSymbolKeyedAnnotation(String name, Object value)
    {
        getDescribed().put(Symbol.valueOf(name), value);
    }

    public void setUnsignedLongKeyedAnnotation(UnsignedLong name, Object value)
    {
        throw new UnsupportedOperationException("UnsignedLong keys are currently reserved");
    }
}
