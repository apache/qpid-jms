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

/**
 * Interface for JmsResources that are part of a running transaction to use
 * to register for notifications of transaction commit and rollback in order
 * to execute specific actions.
 *
 * One such use of this might be for a consumer to register a synchronization
 * when it is closed while it's parent session is still operating inside a
 * transaction.  The Consumer can close itself following the commit or rollback
 * of the running Transaction.
 */
public abstract class JmsTransactionSynchronization {

    /**
     * Called once before the synchronization is added to the set
     * of synchronizations held for a pending TX.  The caller can
     * check TX state and react accordingly.  If the resource finds
     * that is does not need to be added to the TX it can return false
     * to indicate such.
     *
     * @param context
     *        reference to the transaction context.
     *
     * @return true if the synchronization should be added to the TX.
     *
     * @throws Exception if an error occurs during the event.
     */
    public boolean validate(JmsTransactionContext context) throws Exception {
        return true;
    }

    /**
     * Called after a successful commit of the current Transaction.
     *
     * @throws Exception if an error occurs during the event.
     */
    public void afterCommit() throws Exception {
    }

    /**
     * Called after the current transaction has been rolled back either
     * by a call to rollback or by a failure to complete a commit operation.
     *
     * @throws Exception if an error occurs during the event.
     */
    public void afterRollback() throws Exception {
    }
}
