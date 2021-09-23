/*
 * Copyright (C) 2017-2021 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adhoc.flight.client;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static com.adhoc.flight.client.AdhocFlightClient.outputRootBinaryDataToStream;
import static com.adhoc.flight.client.AdhocFlightClient.unifyBatchesIntoSingleRoot;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.apache.arrow.vector.types.Types.MinorType.UINT1;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AdhocFlightClient}.
 */
@RunWith(MockitoJUnitRunner.class)
public final class AdhocFlightClientTest {

  private static final Field COLUMN_FIELD = Field.nullable("col", UINT1.getType());
  private static final Schema SCHEMA = new Schema(singletonList(COLUMN_FIELD));
  private static final int EXPECTED_BATCH_COUNT = Byte.MAX_VALUE;
  private static final int EXPECTED_ROW_COUNT = Short.MAX_VALUE;
  private static final BufferAllocator ALLOCATOR = new RootAllocator();
  private static final VectorSchemaRoot EXPECTED_FINAL_ROOT = VectorSchemaRoot.create(SCHEMA, ALLOCATOR);
  private static final List<Callable<VectorSchemaRoot>> ROOT_BATCH_PROVIDERS = new ArrayList<>(EXPECTED_BATCH_COUNT);
  private static final Random RANDOM = new Random(Long.SIZE);
  private final Queue<VectorSchemaRoot> responseRoots = new LinkedList<>();
  private final AtomicInteger currentBatch = new AtomicInteger(-1);
  private VectorSchemaRoot unifiedBatches;
  @Mock
  private FlightStream flightStream;
  @Rule
  public ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUpBeforeClass() {
    populateExpectedRoot();
    sliceExpectedRootIntoManyBatchesAndRegister();
  }

  private static void populateExpectedRoot() {
    final UInt1Vector col = ((UInt1Vector) EXPECTED_FINAL_ROOT.getVector(COLUMN_FIELD));
    range(0, EXPECTED_BATCH_COUNT).forEach(i -> col.setSafe(i, RANDOM.nextInt()));
    EXPECTED_FINAL_ROOT.setRowCount(EXPECTED_ROW_COUNT);
  }

  private static void sliceExpectedRootIntoManyBatchesAndRegister() {
    final int rowCount = EXPECTED_FINAL_ROOT.getRowCount();
    final int rowsPerBatch = rowCount / EXPECTED_BATCH_COUNT;
    final int remainderForLastBatch = rowCount % EXPECTED_BATCH_COUNT;
    final int lastBatchIndex = EXPECTED_BATCH_COUNT - 1;
    final IntConsumer registerSlice =
        slice ->
            ROOT_BATCH_PROVIDERS.add(
                () -> EXPECTED_FINAL_ROOT.slice(
                    slice * rowsPerBatch,
                    rowsPerBatch + ((slice == lastBatchIndex) ? remainderForLastBatch : 0)));
    range(0, lastBatchIndex).forEach(registerSlice);
    registerSlice.accept(lastBatchIndex);
  }

  @Before
  public void setUp() throws Exception {
    when(flightStream.getSchema()).thenReturn(SCHEMA);
    when(flightStream.getRoot()).thenAnswer(flightStreamMock -> responseRoots.poll());
    when(flightStream.next()).thenAnswer(flightStreamMock -> {
      final int currentBatch = this.currentBatch.incrementAndGet();
      if (currentBatch >= EXPECTED_BATCH_COUNT) return false;
      responseRoots.add(ROOT_BATCH_PROVIDERS.get(currentBatch).call());
      return true;
    });
    unifiedBatches = unifyBatchesIntoSingleRoot(flightStream);
  }

  @After
  public void tearDown() throws Exception {
    responseRoots.forEach(AutoCloseables::closeNoChecked);
    close(unifiedBatches, flightStream);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    close(EXPECTED_FINAL_ROOT, ALLOCATOR);
  }

  @Test
  public void testUnifiedBatchesShouldMatchOriginalRoot() {
    collector.checkThat(unifiedBatches.contentToTSVString(), is(EXPECTED_FINAL_ROOT.contentToTSVString()));
  }

  @Test
  public void testReadBytesFromUnifiedBatchesShouldMatchTheOnesFromUnifiedRoot() throws Exception {
    final byte[] actualBytesFromBatches;
    final byte[] expectedBytesFromRoot;
    try (final ByteArrayOutputStream actualBytesFromUnifiedBatches = new ByteArrayOutputStream();
         final ByteArrayOutputStream expectedBytesFromOriginalRoot = new ByteArrayOutputStream()) {
      outputRootBinaryDataToStream(unifiedBatches, actualBytesFromUnifiedBatches);
      actualBytesFromBatches = actualBytesFromUnifiedBatches.toByteArray();
      outputRootBinaryDataToStream(EXPECTED_FINAL_ROOT, expectedBytesFromOriginalRoot);
      expectedBytesFromRoot = expectedBytesFromOriginalRoot.toByteArray();
    }
    collector.checkThat(actualBytesFromBatches, is(expectedBytesFromRoot));
  }
}
