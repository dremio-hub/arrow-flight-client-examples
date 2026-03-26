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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AdhocFlightClientProjectIdTest {

  @Test
  public void testSetSessionOptions() throws Exception {
    final String mockProjectId = UUID.randomUUID().toString();

    FlightClient flightClient = mock(FlightClient.class);

    AdhocFlightClient client = new AdhocFlightClient(flightClient,
        new RootAllocator(Long.MAX_VALUE),
        new CredentialCallOption(new BasicAuthCredentialWriter("login", "password")),
        mockProjectId);

    final ArgumentCaptor<SetSessionOptionsRequest> argumentCaptor =
        ArgumentCaptor.forClass(SetSessionOptionsRequest.class);

    when(flightClient.setSessionOptions(argumentCaptor.capture(), anyVararg())).thenReturn(
        new SetSessionOptionsResult(new HashMap<>()));

    when(flightClient.getStream(anyObject(), anyVararg())).thenThrow(
        new UnsupportedOperationException());

    byte[] randomBytes = "randomBytes".getBytes();
    when(flightClient.getInfo(anyObject(), anyVararg())).thenReturn(
        new FlightInfo(new Schema(new ArrayList<>()),
            FlightDescriptor.command(randomBytes), Collections.singletonList(
            new FlightEndpoint(new Ticket(randomBytes),
                Location.forGrpcTls("localhost", 1234))), 12L, 12L));

    try {
      // Test the client without the execution stage
      client.runQuery("any query", null, null, false);
    } catch (UnsupportedOperationException ignored) {
    }

    final List<SetSessionOptionsRequest> capturedRequest = argumentCaptor.getAllValues();
    assertFalse(capturedRequest.isEmpty());
    for (SetSessionOptionsRequest request : capturedRequest) {
      final Map<String, SessionOptionValue> entry = request.getSessionOptions();
      assertTrue(entry.containsKey(AdhocFlightClient.PROJECT_ID_KEY));
    }
  }
}
