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
package org.apache.qpid.jms.impl;

import javax.jms.Destination;

public class DestinationImpl implements Destination
{
    private String _address;

    public DestinationImpl(String address)
    {
        if(address == null)
        {
            throw new IllegalArgumentException("Destination address must not be null");
        }

        _address = address;
    }

    public String getAddress()
    {
        return _address;
    }

    @Override
    public String toString()
    {
        return getAddress();
    }

    @Override
    public int hashCode()
    {
        return getAddress().hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o != null && getClass() == o.getClass())
        {
            return getAddress().equals(((DestinationImpl)o).getAddress());
        }
        else
        {
            return false;
        }
    }
}
