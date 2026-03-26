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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.junit.Test;

public class AdhocFlightClientTokenAuthTest {

  @Test
  public void testPatAuthenticationSkipsHandshake() {
    final FlightClient flightClient = mock(FlightClient.class);
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert("routing_engine", "qa-engine");

    final CredentialCallOption credentialCallOption = AdhocFlightClient.authenticatePatOrAuthToken(
        flightClient,
        "test-token",
        new HeaderCallOption(callHeaders));

    assertNotNull(credentialCallOption);
    verifyZeroInteractions(flightClient);
  }
}
