/*
 * Copyright (C) 2017-2020 Dremio Corporation
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
package com.adhoc.flight;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.adhoc.flight.client.AdhocFlightClient;

/**
 * Test Adhoc Flight Client.
 */
public class TestAdhocFlightClient {
    private static final String HOST = "localhost";
    private static final int PORT = 32010;
    private static final String USERNAME = "dremio";
    private static final String PASSWORD = "dremio123";
    public static final String SIMPLE_QUERY = "select * from (VALUES(1,2,3),(4,5,6))";

    private AdhocFlightClient client;
    private BufferAllocator allocator;

    @Before
    public void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @After
    public void shutdown() throws Exception {
        AutoCloseables.close(client);
        allocator = null;
        client = null;
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSimpleQuery() throws Exception {
        createBasicFlightClient(HOST, PORT, USERNAME, PASSWORD);
        client.runQuery(SIMPLE_QUERY);
    }

    @Test
    public void testBadHostname() {
        final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
                () -> createBasicFlightClient("1.1.1.1", PORT, USERNAME, PASSWORD));
        assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
    }

    @Test
    public void testBadPort() {
        final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
                () -> createBasicFlightClient(HOST, 1111, USERNAME, PASSWORD));
        assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
    }

    @Test
    public void testBadPassword() {
        final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
                () -> createBasicFlightClient(HOST, PORT, USERNAME, "BAD_PASSWORD"));
        assertEquals(FlightStatusCode.UNAUTHENTICATED, fre.status().code());
    }

    @Test
    public void testNonExistentUser() {
        final FlightRuntimeException fre = assertThrows(FlightRuntimeException.class,
                () -> createBasicFlightClient(HOST, PORT, "BAD_USER", PASSWORD));
        assertEquals(FlightStatusCode.UNAUTHENTICATED, fre.status().code());

    }

    private void createBasicFlightClient(String host, int port, String user, String pass) {
        client = AdhocFlightClient.getBasicClient(allocator, host, port, user, pass);
    }
}
