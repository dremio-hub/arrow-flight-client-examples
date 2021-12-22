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

package com.adhoc.flight;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adhoc.flight.client.AdhocFlightClient;
import com.google.common.base.Strings;

public class TestWithMockServer {
  private static final String USERNAME = "dremio";
  private static final String PASSWORD = "dremio123";
  private static final BufferAllocator ALLOCATOR = new RootAllocator();

  private static FlightServer server;
  private AdhocFlightClient client;
  private HeaderServerMiddlewareFactory headerServerMiddlewareFactory;

  @Before
  public void setup() throws IOException {
    server = getServer();
    server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.close();
  }

  @Test
  public void test() throws IOException {
    final CallHeaders callHeaders = new FlightCallHeaders();
    callHeaders.insert("engine", "123");

    final HeaderCallOption callOption = new HeaderCallOption(callHeaders);

    client = AdhocFlightClient
      .getBasicClient(ALLOCATOR, "localhost", 9999, USERNAME, PASSWORD, null, callOption,
        null);



    assertEquals(1, 1);
  }

  public FlightServer getServer() throws IOException {

    NoOpFlightProducer producer = new NoOpFlightProducer();
    final Location location = Location.forGrpcInsecure("localhost", 10000);

    headerServerMiddlewareFactory = new HeaderServerMiddlewareFactory();

    server = FlightServer.builder(ALLOCATOR, location, producer)
      .headerAuthenticator(new GeneratedBearerTokenAuthenticator(new BasicCallHeaderAuthenticator(this::validate)))
      .middleware(FlightServerMiddleware.Key.of("test"), headerServerMiddlewareFactory)
      .build();


    return server;
  }

  private CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    if (Strings.isNullOrEmpty(username)) {
      throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
    }
    final String identity;
    if (USERNAME.equals(username) && PASSWORD.equals(password)) {
      identity = USERNAME;
    } else {
      throw CallStatus.UNAUTHENTICATED.withDescription("Username or password is invalid.").toRuntimeException();
    }
    return () -> identity;
  }

  static class HeaderServerMiddlewareFactory implements FlightServerMiddleware.Factory<HeaderServerMiddleware> {
    Map<String, String> headers = null;

    @Override
    public HeaderServerMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders,
        RequestContext context) {
      headers = new HashMap<>();
      return new HeaderServerMiddleware(this);
    }
  }

  static class HeaderServerMiddleware implements FlightServerMiddleware {
    private final HeaderServerMiddlewareFactory factory;

    public HeaderServerMiddleware(HeaderServerMiddlewareFactory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.keys().forEach( key -> {
        factory.headers.put(key, outgoingHeaders.get(key));
      });
    }

    @Override
    public void onCallCompleted(CallStatus status) {

    }

    @Override
    public void onCallErrored(Throwable err) {

    }
  }
}
