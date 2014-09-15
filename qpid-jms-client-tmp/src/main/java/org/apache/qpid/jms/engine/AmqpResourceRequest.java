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
import java.util.concurrent.CountDownLatch;

public class AmqpResourceRequest<T>
{
    private CountDownLatch _latch;
    private volatile boolean _success;//TODO: delete?
    private T _result;
    private Throwable _cause;

    public AmqpResourceRequest()
    {
        _latch = new CountDownLatch(1);
    }

    public boolean isComplete()
    {
        return _latch.getCount() == 0;
    }

    public boolean isSuccess()
    {
        return _success;
    }

    public void onSuccess(T result)
    {
        _success = true;
        markCompletion();
    }

    public void onFailure(Throwable cause)
    {
        _success = false;
        _cause = cause;
        markCompletion();
    }

    public T getResult() throws IOException
    {
        //TODO: timeout
        try
        {
            _latch.await();
        }
        catch (InterruptedException e)
        {
            Thread.interrupted();
            throw new IOException("Unable to retrieve result due to interruption", e);
        }

        if(_cause !=null)
        {
            throw new IOException("Unable to retrieve result due to failure", _cause);
        }

        return _result;
    }

    private void markCompletion()
    {
        _latch.countDown();
    }
}
