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

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import com.adhoc.flight.utils.PrintUtils;

/**
 * Adhoc Flight Client encapsulating an active FlightClient and a corresponding
 * CredentialCallOption with a bearer token for subsequent FlightRPC requests.
 */
public class AdhocFlightClient implements AutoCloseable {
    private final FlightClient client;
    private final CredentialCallOption bearerToken;

    public AdhocFlightClient(FlightClient client, CredentialCallOption bearerToken) {
        this.client = client;
        this.bearerToken = bearerToken;
    }

    /**
     * Creates a FlightClient connected to the Dremio server with encrypted TLS connection.
     *
     * @param allocator the BufferAllocator.
     * @param host the Dremio host.
     * @param port the Dremio port where Flight Server Endpoint is running on.
     * @param user the Dremio username.
     * @param pass the corresponding password.
     * @param keyStorePath path to the JKS.
     * @param keyStorePass the password to the JKS.
     * @param clientProperties the client properties to set during authentication.
     * @return an AdhocFlightClient encapsulating the client instance and CallCredentialOption
     *         with bearer token for subsequent FlightRPC requests.
     * @throws Exception RuntimeException if unable to access JKS with provided information.
     */
    public static AdhocFlightClient getEncryptedClient(BufferAllocator allocator,
                                                       String host, int port,
                                                       String user, String pass,
                                                       String keyStorePath,
                                                       String keyStorePass,
                                                       HeaderCallOption clientProperties) throws Exception {
        final ClientIncomingAuthHeaderMiddleware.Factory factory =
                new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

        final FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcTls(host, port))
                .intercept(factory)
                .useTls()
                .trustedCertificates(EncryptedConnectionUtils.getCertificateStream(
                        keyStorePath, keyStorePass))
                .build();
        return new AdhocFlightClient(client, authenticate(client, user, pass, factory, clientProperties));
    }

    /**
     * Creates a FlightClient connected to the Dremio server with an unencrypted connection.
     *
     * @param allocator the BufferAllocator.
     * @param host the Dremio host.
     * @param port the Dremio port where Flight Server Endpoint is running on.
     * @param user the Dremio username.
     * @param pass the corresponding password.
     * @param clientProperties the client properties to set during authentication.
     * @return an AdhocFlightClient encapsulating the client instance and CallCredentialOption
     *         with bearer token for subsequent FlightRPC requests.
     */
    public static AdhocFlightClient getBasicClient(BufferAllocator allocator,
                                                   String host, int port,
                                                   String user, String pass,
                                                   HeaderCallOption clientProperties) {
        final ClientIncomingAuthHeaderMiddleware.Factory factory =
                new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

        final FlightClient client = FlightClient.builder()
                .allocator(allocator)
                .location(Location.forGrpcInsecure(host, port))
                .intercept(factory)
                .build();
        return new AdhocFlightClient(client, authenticate(client, user, pass, factory, clientProperties));
    }

    /**
     * Helper method to authenticate provided FlightClient instance against a Dremio Flight Server Endpoint.
     *
     * @param client the FlightClient instance to connect to Dremio.
     * @param user the Dremio username.
     * @param pass the corresponding Dremio password
     * @param factory the factory to create ClientIncomingAuthHeaderMiddleware.
     * @param clientProperties client properties to set during authentication.
     * @return CredentialCallOption encapsulating the bearer token to use in subsequent requests.
     */
    public static CredentialCallOption authenticate(FlightClient client,
                                                    String user, String pass,
                                                    ClientIncomingAuthHeaderMiddleware.Factory factory,
                                                    HeaderCallOption clientProperties) {
        final List<CallOption> callOptions = new ArrayList<>();

        // Add CredentialCallOption for authentication.
        callOptions.add(new CredentialCallOption(new BasicAuthCredentialWriter(user, pass)));

        // If provided, add client properties to CallOptions
        if (clientProperties != null) {
            callOptions.add(clientProperties);
        }

        // Perform handshake
        client.handshake(callOptions.toArray(new CallOption[callOptions.size()]));
        return factory.getCredentialCallOption();
    }

    /**
     * Make a FlightRPC getInfo request with the given query and client properties.
     *
     * @param query the query to retrieve FlightInfo for.
     * @param options the client properties to execute this request with.
     * @return a FlightInfo object.
     */
    public FlightInfo getInfo(String query, CallOption ... options) {
        return client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)), options);
    }

    /**
     * Make a FlightRPC getStream request based on the provided FlightInfo object. Retrieves
     * result of the query previously prepared with getInfo.
     *
     * @param flightInfo the FlightInfo object encapsulating information for the server to identify
     *                   the prepared statement with.
     * @param options the client properties to execute this request with.
     * @return a stream of results.
     */
    public FlightStream getStream(FlightInfo flightInfo, CallOption... options) {
        return client.getStream(flightInfo.getEndpoints().get(0).getTicket(), options);
    }

    /**
     * Make FlightRPC requests to the Dremio Flight Server Endpoint to retrieve results of the
     * provided SQL query.
     *
     * @param query the SQL query to execute.
     * @param headerCallOption client properties to execute provided SQL query with.
     * @return query results as a list of Object arrays.
     * @throws Exception if error occurs during query execution.
     */
    public List<Object[]> runQuery(String query, HeaderCallOption headerCallOption) throws Exception {
        final FlightInfo flightInfo = getInfo(query, bearerToken, headerCallOption);
        final FlightStream stream = getStream(flightInfo, bearerToken, headerCallOption);
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
