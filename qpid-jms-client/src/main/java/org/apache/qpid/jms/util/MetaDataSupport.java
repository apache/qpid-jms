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
package org.apache.qpid.jms.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDataSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MetaDataSupport.class);

    public static final String PROVIDER_NAME = "QpidJMS";
    public static final String PROVIDER_VERSION;
    public static final int PROVIDER_MAJOR_VERSION;
    public static final int PROVIDER_MINOR_VERSION;
    public static final String PLATFORM_DETAILS = buildPlatformDetails();

    static {
        String version = "unknown";
        int major = 0;
        int minor = 0;
        try {
            Package p = Package.getPackage(MetaDataSupport.class.getPackage().getName());
            if (p != null) {
                version = p.getImplementationVersion();
                Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+).*");
                Matcher m = pattern.matcher(version);
                if (m.matches()) {
                    major = Integer.parseInt(m.group(1));
                    minor = Integer.parseInt(m.group(2));
                }
            }
        } catch (Throwable e) {
            LOG.trace("Problem generating primary version details", e);

            InputStream in = null;
            String path = MetaDataSupport.class.getPackage().getName().replace(".", "/");
            if ((in = MetaDataSupport.class.getResourceAsStream("/" + path + "/version.txt")) != null) {
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII));
                    version = reader.readLine();
                    Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+).*");
                    Matcher m = pattern.matcher(version);
                    if (m.matches()) {
                        major = Integer.parseInt(m.group(1));
                        minor = Integer.parseInt(m.group(2));
                    }
                    reader.close();
                } catch(Throwable err) {
                    LOG.trace("Problem generating fallback version details", err);
                }
            }
        }

        PROVIDER_VERSION = version;
        PROVIDER_MAJOR_VERSION = major;
        PROVIDER_MINOR_VERSION = minor;
    }

    private static String buildPlatformDetails() {
        String details = "unknown";
        try {
            StringBuilder platformInfo = new StringBuilder(128);

            platformInfo.append("JVM: ");
            platformInfo.append(System.getProperty("java.version"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("java.vm.version"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("java.vendor"));
            platformInfo.append(", OS: ");
            platformInfo.append(System.getProperty("os.name"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("os.version"));
            platformInfo.append(", ");
            platformInfo.append(System.getProperty("os.arch"));

            details = platformInfo.toString();
        } catch (Throwable e) {
            LOG.trace("Problem generating platform details string", e);
        }

        return details;
    }
}
