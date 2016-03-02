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
package org.apache.qpid.jms.provider.amqp;

/**
 * Interface for any object that can will manage the lifetime of AmqpResource
 * based object instances.
 */
public interface AmqpResourceParent {

    /**
     * Adds the given resource as a child of this resource so that it's
     * lifetime becomes managed by that of its parent.
     *
     * @param resource
     *      The AmqpResource that is a child of this one.
     */
    void addChildResource(AmqpResource resource);

    /**
     * Removes the given resource from the registered child resources
     * managed by this one.
     *
     * @param resource
     *      The AmqpResource that is no longer a child of this one.
     */
    void removeChildResource(AmqpResource resource);

    /**
     * @return a reference to the root AmqpProvider.
     */
    AmqpProvider getProvider();

}
