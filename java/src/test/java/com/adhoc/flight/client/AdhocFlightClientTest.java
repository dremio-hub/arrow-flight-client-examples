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

import static com.adhoc.flight.client.AdhocFlightClient.readBytesFromStreamRoot;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.apache.arrow.vector.types.Types.MinorType.UINT1;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
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
  private static final int BATCH_COUNT = 10;
  private static final List<Consumer<VectorSchemaRoot>> BATCHES = new ArrayList<>(BATCH_COUNT);
  private final List<String> expectedTsvContentsForBatches = new ArrayList<>(BATCH_COUNT);
  private final BufferAllocator allocator = new RootAllocator();
  private final VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(SCHEMA, allocator);
  private final AtomicInteger currentBatch = new AtomicInteger();
  @Mock
  private FlightStream flightStream;
  @Rule
  public ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUpBeforeClass() {
    IntStream.range(0, BATCH_COUNT)
        .forEach(i -> BATCHES.add(root -> ((UInt1Vector) root.getVector(COLUMN_FIELD)).setSafe(i, i)));
  }

  @Before
  public void setUp() {
    when(flightStream.getRoot()).thenReturn(vectorSchemaRoot);
    when(flightStream.next()).thenAnswer(flightStreamMock -> {
      final int currentBatch = this.currentBatch.incrementAndGet();
      if (currentBatch < BATCH_COUNT) {
        BATCHES.get(currentBatch).accept(vectorSchemaRoot);
        expectedTsvContentsForBatches.add(vectorSchemaRoot.contentToTSVString());
        return true;
      }
      return false;
    });
    vectorSchemaRoot.setRowCount(BATCH_COUNT);
  }

  @After
  public void tearDown() throws Exception {
    close(vectorSchemaRoot, flightStream, allocator);
  }

  @Test
  public void readBytesFromStreamRootNonNullOutputStreamTest() throws IOException {
    final byte[] bytes;
    final List<String> actualTsvContentsFromBatches = new ArrayList<>(BATCH_COUNT);
    try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      final AtomicInteger iterationCount = new AtomicInteger();
      readBytesFromStreamRoot(
          flightStream,
          byteArrayOutputStream,
          singletonList(root ->
              LOGGER.info(
                  format("Expected batch #%d:%n%s", iterationCount.getAndIncrement(), root.contentToTSVString()))));
      bytes = byteArrayOutputStream.toByteArray();
    }
    try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
         final ArrowStreamReader arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, allocator);
         final VectorSchemaRoot vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot()) {
      for (int i = 0; arrowStreamReader.loadNextBatch(); i++) {
        final String batchTsvContents = vectorSchemaRoot.contentToTSVString();
        actualTsvContentsFromBatches.add(batchTsvContents);
        LOGGER.info(format("Actual batch #%d:%n%s", i, batchTsvContents));
      }
    }
    collector.checkThat(currentBatch.get(), is(BATCH_COUNT));
    collector.checkThat(actualTsvContentsFromBatches, is(expectedTsvContentsForBatches));
  }

  @Test
  public void readBytesFromStreamRootNullOutputStreamTest() {
    final AtomicInteger iterationCount = new AtomicInteger();
    collector.checkSucceeds(() -> {
      readBytesFromStreamRoot(
          flightStream,
          null,
          singletonList(root ->
              LOGGER.info(
                  format("Expected batch #%d:%n%s", iterationCount.getAndIncrement(), root.contentToTSVString()))));
      return (Void) null;
    });
    collector.checkThat(currentBatch.get(), is(0));
  }
}
