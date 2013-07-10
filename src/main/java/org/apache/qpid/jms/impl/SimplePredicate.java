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
 */
package org.apache.qpid.jms.impl;

import java.util.Arrays;


abstract class SimplePredicate implements Predicate
{
    private final String _description;
    private final Object[] _predicatedObjects;

    public SimplePredicate()
    {
        this("");
    }

    /**
     * @param description human-readable description. Useful for logging in case things go wrong.
     * @param predicatedObject provided as a convenient way for toString() can hint at what is being predicated
     */
    public SimplePredicate(String description, Object... predicatedObjects)
    {
        _description = description;
        _predicatedObjects = predicatedObjects;
    }

    /** intended to be overridden to provide more useful information */
    protected Object getCurrentState()
    {
        return getPredicatedObjects();
    }

    /**
     * Returns the predicated objects as a list, or null if none exist
     */
    protected Object getPredicatedObjects()
    {
        if(_predicatedObjects == null)
        {
            return null;
        }
        else
        {
            return Arrays.asList(_predicatedObjects);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SimplePredicate [_description=").append(_description)
            .append(", currentState=").append(getCurrentState()).append("]");
        return builder.toString();
    }
}
