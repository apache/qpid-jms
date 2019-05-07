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

import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;

/**
 * Wraps around the proton Header object and provides an ability to
 * determine if the Header can be optimized out of message encodes
 */
public final class AmqpHeader {

    private static final int DEFAULT_PRIORITY = 4;
    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static byte DURABLE = 1;
    private static byte PRIORITY = 2;
    private static byte TIME_TO_LIVE = 4;
    private static byte FIRST_ACQUIRER = 8;
    private static byte DELIVERY_COUNT = 16;

    private byte modified = 0;

    private Boolean durable;
    private UnsignedByte priority;
    private UnsignedInteger timeToLive;
    private Boolean firstAcquirer;
    private UnsignedInteger deliveryCount;

    public AmqpHeader() {}

    public AmqpHeader(AmqpHeader header) {
        setHeader(header);
    }

    public AmqpHeader(Header header) {
        setHeader(header);
    }

    public void setHeader(Header header) {
        if (header != null) {
            setDurable(header.getDurable());
            setPriority(header.getPriority());
            setTimeToLive(header.getTtl());
            setFirstAcquirer(header.getFirstAcquirer());
            setDeliveryCount(header.getDeliveryCount());
        }
    }

    public void setHeader(AmqpHeader header) {
        if (header != null) {
            modified = header.modified;
            durable = header.durable;
            priority = header.priority;
            timeToLive = header.timeToLive;
            firstAcquirer = header.firstAcquirer;
            deliveryCount = header.deliveryCount;
        }
    }

    public Header getHeader() {
        Header result = null;

        if (!isDefault()) {
            result = new Header();
            // As we are now definitely sending a Header, always
            // populate the durable field explicitly rather than
            // potentially default it if false.
            if(Boolean.TRUE.equals(durable)) {
                result.setDurable(Boolean.TRUE);
            }
            else {
                result.setDurable(Boolean.FALSE);
            }

            result.setPriority(priority);
            result.setFirstAcquirer(firstAcquirer);
            result.setTtl(timeToLive);
            result.setDeliveryCount(deliveryCount);
        }

        return result;
    }

    //----- Query the state of the Header object -----------------------------//

    public boolean isDefault() {
        return modified == 0;
    }

    public boolean nonDefaultDurable() {
        return (modified & DURABLE) == DURABLE;
    }

    public boolean nonDefaultPriority() {
        return (modified & PRIORITY) == PRIORITY;
    }

    public boolean nonDefaultTimeToLive() {
        return (modified & TIME_TO_LIVE) == TIME_TO_LIVE;
    }

    public boolean nonDefaultFirstAcquirer() {
        return (modified & FIRST_ACQUIRER) == FIRST_ACQUIRER;
    }

    public boolean nonDefaultDeliveryCount() {
        return (modified & DELIVERY_COUNT) == DELIVERY_COUNT;
    }

    //----- Access the AMQP Header object ------------------------------------//

    public boolean isDurable() {
        return Boolean.TRUE.equals(durable);
    }

    public void setDurable(Boolean value) {
        if (Boolean.TRUE.equals(value)) {
            modified |= DURABLE;
            durable = value;
        } else {
            modified &= ~DURABLE;
            durable = null;
        }
    }

    public int getPriority() {
        if (priority != null) {
            int scaled = priority.intValue();
            if (scaled > 9) {
                scaled = 9;
            }

            return scaled;
        }

        return DEFAULT_PRIORITY;
    }

    public void setPriority(UnsignedByte value) {
        if (value == null || value.intValue() == DEFAULT_PRIORITY) {
            modified &= ~PRIORITY;
            priority = null;
        } else {
            modified |= PRIORITY;
            priority = value;
        }
    }

    public void setPriority(int priority) {
        if (priority == DEFAULT_PRIORITY) {
            setPriority(null);
        } else {
            byte scaled = (byte) priority;
            if (priority < 0) {
                scaled = 0;
            } else if (priority > 9) {
                scaled = 9;
            }

            setPriority(UnsignedByte.valueOf(scaled));
        }
    }

    public long getTimeToLive() {
        return timeToLive == null ? 0l : timeToLive.longValue();
    }

    public void setTimeToLive(UnsignedInteger value) {
        if (value == null || UnsignedInteger.ZERO.equals(value)) {
            modified &= ~TIME_TO_LIVE;
            timeToLive = null;
        } else {
            modified |= TIME_TO_LIVE;
            timeToLive = value;
        }
    }

    public void setTimeToLive(long timeToLive) {
        if (timeToLive > 0 && timeToLive < UINT_MAX) {
            setTimeToLive(UnsignedInteger.valueOf(timeToLive));
        } else {
            setTimeToLive(null);
        }
    }

    public boolean isFirstAcquirer() {
        return Boolean.TRUE.equals(firstAcquirer);
    }

    public void setFirstAcquirer(Boolean value) {
        if (Boolean.TRUE.equals(value)) {
            modified |= FIRST_ACQUIRER;
            firstAcquirer = Boolean.TRUE;
        } else {
            modified &= ~FIRST_ACQUIRER;
            firstAcquirer = null;
        }
    }

    public int getDeliveryCount() {
        return deliveryCount == null ? 0 : deliveryCount.intValue();
    }

    public void setDeliveryCount(UnsignedInteger value) {
        if (value == null || UnsignedInteger.ZERO.equals(value)) {
            modified &= ~DELIVERY_COUNT;
            deliveryCount = null;
        } else {
            modified |= DELIVERY_COUNT;
            deliveryCount = value;
        }
    }

    public void setDeliveryCount(int count) {
        if (count == 0) {
            setDeliveryCount(null);
        } else {
            setDeliveryCount(UnsignedInteger.valueOf(count));
        }
    }

    @Override
    public String toString() {
        return "AmqpHeader {" +
               "durable=" + durable +
               ", priority=" + priority +
               ", ttl=" + timeToLive +
               ", firstAcquirer=" + firstAcquirer +
               ", deliveryCount=" + deliveryCount + " }";
    }
}
