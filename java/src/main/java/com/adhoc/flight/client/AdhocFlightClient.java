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
package com.adhoc.flight.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import com.adhoc.flight.utils.PrintUtils;

/**
 * Adhoc Flight Client.
 */
public class AdhocFlightClient implements AutoCloseable {
    private final FlightClient client;
    private final CredentialCallOption bearerToken;

    public AdhocFlightClient(FlightClient client, CredentialCallOption bearerToken) {
        this.client = client;
        this.bearerToken = bearerToken;
    }

    public static AdhocFlightClient getEncryptedClient(BufferAllocator allocator, String host, int port,
                                                       String user, String pass, String keyStorePath,
                                                       String keyStorePass) throws Exception {
        final FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcTls(host, port))
                .useTls()
                .trustedCertificates(EncryptedConnectionUtils.getCertificateStream(
                        keyStorePath, keyStorePass))
                .build();
        return new AdhocFlightClient(client, authenticate(client, user, pass));
    }

    public static AdhocFlightClient getBasicClient(BufferAllocator allocator, String host, int port,
                                                   String user, String pass) {
        final FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure(host, port))
                .build();
        return new AdhocFlightClient(client, authenticate(client, user, pass));
    }

    private static CredentialCallOption authenticate(FlightClient client, String user, String pass) {
        return client.authenticateBasicToken(user, pass).get();
    }

    private FlightInfo getFlightInfo(String query) {
        return client.getInfo(
                FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)), bearerToken);
    }

    private FlightStream getStream(FlightInfo flightInfo) {
        return client.getStream(flightInfo.getEndpoints().get(0).getTicket(), bearerToken);
    }

    public List<Object[]> runQuery(String query) throws Exception {
        final FlightInfo flightInfo = getFlightInfo(query);
        final FlightStream stream = getStream(flightInfo);
        final List<Object[]> values = new ArrayList<>();
        final List<Field> fields = stream.getSchema().getFields();
        final int columnCount = fields.size();
        while (stream.next()) {
            VectorSchemaRoot root = stream.getRoot();
            final long rowCount = root.getRowCount();

            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final Field field = fields.get(columnIndex);

                final FieldVector fieldVector = root.getVector(field.getName());

                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    if (values.size() - 1 < rowIndex) {
                        values.add(new Object[columnCount]);
                    }
                    final Object[] rowValues = values.get(rowIndex);
                    final Object value = fieldVector.getObject(rowIndex);
                    rowValues[columnIndex] = value;
                }
            }
            AutoCloseables.close(root);
        }
        AutoCloseables.close(stream);
        return values;
    }

    public void close() {
        try {
            client.close();
        } catch (InterruptedException ex) {
            PrintUtils.printExceptionOnClosed(ex);
        }
    }
}