/**
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
package org.apache.qpid.jms.message.facade.defaults;

import org.apache.qpid.jms.message.facade.JmsBytesMessageFacade;
import org.fusesource.hawtbuf.Buffer;

/**
 * A default implementation of the JmsBytesMessageFacade that simply holds a raw Buffer
 */
public final class JmsDefaultBytesMessageFacade extends JmsDefaultMessageFacade implements JmsBytesMessageFacade {

    private Buffer content;

    @Override
    public JmsMsgType getMsgType() {
        return JmsMsgType.BYTES;
    }

    @Override
    public JmsDefaultBytesMessageFacade copy() {
        JmsDefaultBytesMessageFacade copy = new JmsDefaultBytesMessageFacade();
        copyInto(copy);
        if (this.content != null) {
            copy.setContent(this.content.deepCopy());
        }

        return copy;
    }

    @Override
    public boolean isEmpty() {
        if (content == null || content.length() == 0) {
            return true;
        }

        return false;
    }

    @Override
    public void clearBody() {
        this.content = null;
    }

    @Override
    public Buffer getContent() {
        return content;
    }

    @Override
    public void setContent(Buffer content) {
        this.content = content;
    }
}
