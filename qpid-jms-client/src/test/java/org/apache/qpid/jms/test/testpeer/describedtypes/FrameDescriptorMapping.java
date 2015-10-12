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
 *
 */
package org.apache.qpid.jms.test.testpeer.describedtypes;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

public class FrameDescriptorMapping {
    private static Map<Object, Object> descriptorMappings = new HashMap<Object, Object>();

    static {
        // AMQP frames (type 0x00)
        addMapping(OpenFrame.DESCRIPTOR_SYMBOL, OpenFrame.DESCRIPTOR_CODE);
        addMapping(BeginFrame.DESCRIPTOR_SYMBOL, BeginFrame.DESCRIPTOR_CODE);
        addMapping(AttachFrame.DESCRIPTOR_SYMBOL, AttachFrame.DESCRIPTOR_CODE);
        addMapping(FlowFrame.DESCRIPTOR_SYMBOL, FlowFrame.DESCRIPTOR_CODE);
        addMapping(TransferFrame.DESCRIPTOR_SYMBOL, TransferFrame.DESCRIPTOR_CODE);
        addMapping(DispositionFrame.DESCRIPTOR_SYMBOL, DispositionFrame.DESCRIPTOR_CODE);
        addMapping(DetachFrame.DESCRIPTOR_SYMBOL, DetachFrame.DESCRIPTOR_CODE);
        addMapping(EndFrame.DESCRIPTOR_SYMBOL, EndFrame.DESCRIPTOR_CODE);
        addMapping(CloseFrame.DESCRIPTOR_SYMBOL, CloseFrame.DESCRIPTOR_CODE);

        // SASL frames (type 0x01)
        addMapping(SaslMechanismsFrame.DESCRIPTOR_SYMBOL, SaslMechanismsFrame.DESCRIPTOR_CODE);
        addMapping(SaslInitFrame.DESCRIPTOR_SYMBOL, SaslInitFrame.DESCRIPTOR_CODE);
        addMapping(SaslChallengeFrame.DESCRIPTOR_SYMBOL, SaslChallengeFrame.DESCRIPTOR_CODE);
        addMapping(SaslResponseFrame.DESCRIPTOR_SYMBOL, SaslResponseFrame.DESCRIPTOR_CODE);
        addMapping(SaslOutcomeFrame.DESCRIPTOR_SYMBOL, SaslOutcomeFrame.DESCRIPTOR_CODE);
    }

    private static void addMapping(Symbol descriptorSymbol, UnsignedLong descriptorCode) {
        descriptorMappings.put(descriptorSymbol, descriptorCode);
        descriptorMappings.put(descriptorCode, descriptorSymbol);
    }

    public static Object lookupMapping(Object descriptor) {
        Object mapping = descriptorMappings.get(descriptor);
        if (mapping == null) {
            mapping = "UNKNOWN";
        }

        return mapping;
    }
}
