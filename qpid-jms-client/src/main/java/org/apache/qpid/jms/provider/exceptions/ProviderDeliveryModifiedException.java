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
package org.apache.qpid.jms.provider.exceptions;

import org.apache.qpid.jms.provider.ProviderException;
import org.apache.qpid.proton.amqp.messaging.Modified;

/**
 * Thrown when a send fails because the remote modified the delivery
 */
public class ProviderDeliveryModifiedException extends ProviderException {

    private static final long serialVersionUID = 4099784529012859035L;

    private final Modified modification;

    public ProviderDeliveryModifiedException(String message, Modified modification) {
        super(message);

        this.modification = modification;
    }

    public ProviderDeliveryModifiedException(String message, Throwable cause, Modified modification) {
        super(message, cause);

        this.modification = modification;
    }

    public boolean isDeliveryFailed() {
        return modification.getDeliveryFailed() == null ? false : modification.getDeliveryFailed().booleanValue();
    }

    public boolean isUndeliverableHere() {
        return modification.getUndeliverableHere() == null ? false : modification.getUndeliverableHere().booleanValue();
    }
}
