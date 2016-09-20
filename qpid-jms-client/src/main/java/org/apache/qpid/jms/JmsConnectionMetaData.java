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
package org.apache.qpid.jms;

import java.util.Enumeration;
import java.util.Vector;

import javax.jms.ConnectionMetaData;

import org.apache.qpid.jms.util.MetaDataSupport;

/**
 * A <CODE>ConnectionMetaData</CODE> object provides information describing
 * the <CODE>Connection</CODE> object.
 */
public final class JmsConnectionMetaData implements ConnectionMetaData {

    public static final JmsConnectionMetaData INSTANCE = new JmsConnectionMetaData();

    private JmsConnectionMetaData() {}

    /**
     * Gets the JMS API version.
     *
     * @return the JMS API version
     */
    @Override
    public String getJMSVersion() {
        return "2.0";
    }

    /**
     * Gets the JMS major version number.
     *
     * @return the JMS API major version number
     */
    @Override
    public int getJMSMajorVersion() {
        return 2;
    }

    /**
     * Gets the JMS minor version number.
     *
     * @return the JMS API minor version number
     */
    @Override
    public int getJMSMinorVersion() {
        return 0;
    }

    /**
     * Gets the JMS provider name.
     *
     * @return the JMS provider name
     */
    @Override
    public String getJMSProviderName() {
        return MetaDataSupport.PROVIDER_NAME;
    }

    /**
     * Gets the JMS provider version.
     *
     * @return the JMS provider version
     */
    @Override
    public String getProviderVersion() {
        return MetaDataSupport.PROVIDER_VERSION;
    }

    /**
     * Gets the JMS provider major version number.
     *
     * @return the JMS provider major version number
     */
    @Override
    public int getProviderMajorVersion() {
        return MetaDataSupport.PROVIDER_MAJOR_VERSION;
    }

    /**
     * Gets the JMS provider minor version number.
     *
     * @return the JMS provider minor version number
     */
    @Override
    public int getProviderMinorVersion() {
        return MetaDataSupport.PROVIDER_MINOR_VERSION;
    }

    /**
     * Gets an enumeration of the JMSX property names.
     *
     * @return an Enumeration of JMSX property names
     */
    @Override
    public Enumeration<String> getJMSXPropertyNames() {
        Vector<String> jmxProperties = new Vector<String>();
        jmxProperties.add("JMSXUserID");
        jmxProperties.add("JMSXGroupID");
        jmxProperties.add("JMSXGroupSeq");
        jmxProperties.add("JMSXDeliveryCount");
        return jmxProperties.elements();
    }
}
