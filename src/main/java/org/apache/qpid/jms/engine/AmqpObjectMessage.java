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
package org.apache.qpid.jms.engine;

import java.io.IOException;
import java.io.Serializable;

import org.apache.qpid.jms.impl.ClientProperties;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpObjectMessage extends AmqpMessage
{
    private AmqpObjectMessageDelegate _delegate;
    private boolean _useAmqpTypeEncoding = false;

    public AmqpObjectMessage()
    {
        super();
        setContentType(AmqpObjectMessageSerializedDelegate.CONTENT_TYPE);
        setMessageAnnotation(ClientProperties.X_OPT_JMS_MSG_TYPE, ClientProperties.OBJECT_MESSSAGE_TYPE);
        initDelegate(false);
    }

    public AmqpObjectMessage(Message message, Delivery delivery, AmqpConnection amqpConnection, boolean useAmqpTypes)
    {
        super(message, delivery, amqpConnection);

        initDelegate(useAmqpTypes);
    }

    private void initDelegate(boolean useAmqpTypes)
    {
        if(!useAmqpTypes)
        {
            _delegate = new AmqpObjectMessageSerializedDelegate(this);
        }
        else
        {
            _delegate = new AmqpObjectMessageAmqpTypedDelegate(this);
        }
    }

    /**
     * Sets the serialized object as a data section in the underlying message, or
     * clears the body section if null.
     */
    public void setObject(Serializable serializable) throws IOException
    {
        _delegate.setObject(serializable);
    }

    /**
     * Returns the deserialized object, or null if no object data is present.
     */
    public Serializable getObject() throws IllegalStateException, IOException, ClassNotFoundException
    {
        return _delegate.getObject();
    }

    public void setUseAmqpTypeEncoding(boolean useAmqpTypeEncoding) throws ClassNotFoundException, IOException
    {
        if(_useAmqpTypeEncoding != useAmqpTypeEncoding)
        {
            Serializable existingObject = _delegate.getObject();

            AmqpObjectMessageDelegate newDelegate = null;
            if(useAmqpTypeEncoding)
            {
                newDelegate = new AmqpObjectMessageAmqpTypedDelegate(this);
            }
            else if(!useAmqpTypeEncoding)
            {
                newDelegate = new AmqpObjectMessageSerializedDelegate(this);
            }

            newDelegate.setObject(existingObject);

            _delegate = newDelegate;
            _useAmqpTypeEncoding = useAmqpTypeEncoding;

            //TODO: ensure we only set content-type if we are using a Data section
        }
    }
}
