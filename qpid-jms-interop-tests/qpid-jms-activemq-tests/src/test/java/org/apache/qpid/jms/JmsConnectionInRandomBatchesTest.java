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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jms.Connection;

import org.apache.qpid.jms.support.AmqpTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for creation of several open connections in a series of randomly
 * sized batches over time.
 */
public class JmsConnectionInRandomBatchesTest extends AmqpTestSupport  {

    private final List<Connection> batch = new ArrayList<Connection>();
    private final Random batchSizeGenerator = new Random();

    private final int RANDOM_SIZE_MARKER = -1;
    private final int MAX_BATCH_SIZE = 20;
    private final int MAX_BATCH_ITERATIONS = 10;

    @Override
    @Before
    public void setUp() throws Exception {
        batchSizeGenerator.setSeed(System.nanoTime());
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        doCloseConnectionBatch();
        super.tearDown();
    }

    @Test(timeout = 60 * 1000)
    public void testSingleBatch() throws Exception {
        doCreateConnectionBatch(MAX_BATCH_SIZE);
    }

    @Test(timeout = 60 * 1000)
    public void testCreateManyBatches() throws Exception {
        doCreateConnectionInBatches(MAX_BATCH_ITERATIONS, MAX_BATCH_SIZE);
    }

    @Test(timeout = 60 * 1000)
    public void testCreateRandomSizedBatches() throws Exception {
        doCreateConnectionInBatches(MAX_BATCH_ITERATIONS, RANDOM_SIZE_MARKER);
    }

    private void doCreateConnectionInBatches(int count, int size) throws Exception {
        for (int i = 0; i < count; ++i) {
            if (size != RANDOM_SIZE_MARKER) {
                doCreateConnectionBatch(size);
            } else {
                doCreateConnectionBatch(getNextBatchSize());
            }
            doCloseConnectionBatch();
        }
    }

    private void doCreateConnectionBatch(int size) throws Exception {
        for (int i = 0; i < size; ++i) {
            batch.add(createAmqpConnection());
            batch.get(i).start();
        }
    }

    private void doCloseConnectionBatch() {
        for (Connection connection : batch) {
            try {
                connection.close();
            } catch (Exception ex) {
            }
        }

        batch.clear();
    }

    private int getNextBatchSize() {
        return batchSizeGenerator.nextInt(MAX_BATCH_SIZE) + 1;
    }
}
