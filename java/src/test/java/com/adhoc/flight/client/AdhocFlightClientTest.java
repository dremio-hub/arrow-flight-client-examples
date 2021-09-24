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

import static com.adhoc.flight.client.AdhocFlightClient.writeToOutputStream;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.apache.arrow.util.Preconditions.checkState;
import static org.apache.arrow.vector.types.Types.MinorType.UINT1;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.logging.Logger;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
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

/**
 * Tests for {@link AdhocFlightClient}.
 */
@RunWith(MockitoJUnitRunner.class)
public final class AdhocFlightClientTest {

  private static final Logger LOGGER = Logger.getLogger(AdhocFlightClientTest.class.getName());
  private static final Field COLUMN_FIELD = Field.nullable("col", UINT1.getType());
  private static final Schema SCHEMA = new Schema(singletonList(COLUMN_FIELD));
  private static final int EXPECTED_BATCH_COUNT = 10;
  private static final int EXPECTED_ROW_COUNT = 11;
  private static final BufferAllocator ALLOCATOR = new RootAllocator();
  private static final VectorSchemaRoot EXPECTED_FINAL_ROOT = VectorSchemaRoot.create(SCHEMA, ALLOCATOR);
  private static final List<Callable<VectorSchemaRoot>> ROOT_BATCH_PROVIDERS = new ArrayList<>(EXPECTED_BATCH_COUNT);
  private static final Random RANDOM = new Random(Long.SIZE);
  private final List<VectorSchemaRoot> responseRoots = new ArrayList<>(EXPECTED_BATCH_COUNT);
  private final AtomicInteger currentBatch = new AtomicInteger(-1);
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
  public void setUp() {
    checkState(responseRoots.isEmpty(), "Response roots are not empty. Test results are unreliable.");
    when(flightStream.getSchema()).thenReturn(SCHEMA);
    when(flightStream.getRoot()).thenAnswer(flightStreamMock -> responseRoots.get(currentBatch.get()));
    when(flightStream.next()).thenAnswer(flightStreamMock -> {
      final int currentBatch = this.currentBatch.incrementAndGet();
      if (currentBatch >= EXPECTED_BATCH_COUNT) {
        return false;
      }
      responseRoots.add(ROOT_BATCH_PROVIDERS.get(currentBatch).call());
      return true;
    });
  }

  @After
  public void tearDown() throws Exception {
    responseRoots.forEach(AutoCloseables::closeNoChecked);
    close(flightStream);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    close(EXPECTED_FINAL_ROOT, ALLOCATOR);
  }

  @Test
  public void testBatchesShouldMatchOriginalRoot() throws Exception {
    final byte[] data;
    final List<String> originalBatches = new ArrayList<>(EXPECTED_BATCH_COUNT);
    try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         final OutputStream outputStream = new BufferedOutputStream(byteArrayOutputStream)) {
      final AtomicInteger innerBatchCount = new AtomicInteger();
      writeToOutputStream(
          flightStream, ALLOCATOR, outputStream,
          root -> originalBatches.add(innerBatchCount.getAndIncrement(), root.contentToTSVString()));
      data = byteArrayOutputStream.toByteArray();
    }

    final List<String> actualBatches = new ArrayList<>(EXPECTED_BATCH_COUNT);
    try (final InputStream inputStream = new BufferedInputStream(new ByteArrayInputStream(data));
         final ArrowStreamReader arrowStreamReader = new ArrowStreamReader(inputStream, ALLOCATOR);
         final VectorSchemaRoot parsedRoot = arrowStreamReader.getVectorSchemaRoot()) {
      while (arrowStreamReader.loadNextBatch()) {
        actualBatches.add(parsedRoot.contentToTSVString());
      }
    }

    collector.checkThat(actualBatches, is(originalBatches));
    collector.checkThat(responseRoots.size(), is(allOf(equalTo(actualBatches.size()), equalTo(EXPECTED_BATCH_COUNT))));
    collector.checkThrows(
        IndexOutOfBoundsException.class /* Because it should have already been consumed at this point */,
        () ->
            responseRoots.stream()
                .map(VectorSchemaRoot::contentToTSVString)
                .forEach(
                    content ->
                        LOGGER.warning(format("Query result is truncated. Missing content:%n<%s>", content))));
  }
}
