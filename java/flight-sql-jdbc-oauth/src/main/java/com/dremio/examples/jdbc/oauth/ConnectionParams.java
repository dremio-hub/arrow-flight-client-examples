/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.examples.jdbc.oauth;

import com.beust.jcommander.Parameter;
import java.util.Properties;

final class ConnectionParams {
  @Parameter(names = "--host", description = "Flight SQL hostname")
  String host = "localhost";

  @Parameter(names = "--port", description = "Flight SQL port")
  int port = 32010;

  @Parameter(names = "--query", description = "SQL query to run")
  String query = "SELECT 1 AS example_value";

  @Parameter(names = "--max-rows", description = "Maximum rows to print")
  int maxRows = 10;

  @Parameter(names = "--use-encryption", description = "Enable encrypted connection")
  boolean useEncryption = false;

  @Parameter(names = "--disable-certificate-verification",
      description = "Disable TLS server verification")
  boolean disableCertificateVerification = false;

  @Parameter(names = "--tls-root-certs", description = "PEM file for TLS verification")
  String tlsRootCerts;

  @Parameter(names = "--trust-store", description = "Java trust store path")
  String trustStore;

  @Parameter(names = "--trust-store-password", description = "Trust store password")
  String trustStorePassword;

  @Parameter(names = "--client-certificate", description = "Client mTLS certificate path")
  String clientCertificate;

  @Parameter(names = "--client-key", description = "Client mTLS key path")
  String clientKey;

  @Parameter(names = "--catalog", description = "Default catalog")
  String catalog;

  @Parameter(names = "--help", help = true, description = "Show usage")
  boolean help;

  String connectionUrl() {
    return String.format("jdbc:arrow-flight-sql://%s:%d", host, port);
  }

  Properties toProperties() {
    final Properties properties = new Properties();

    properties.setProperty("useEncryption", Boolean.toString(useEncryption));
    properties.setProperty("disableCertificateVerification",
        Boolean.toString(disableCertificateVerification));

    setIfPresent(properties, "tlsRootCerts", tlsRootCerts);
    setIfPresent(properties, "trustStore", trustStore);
    setIfPresent(properties, "trustStorePassword", trustStorePassword);
    setIfPresent(properties, "clientCertificate", clientCertificate);
    setIfPresent(properties, "clientKey", clientKey);
    setIfPresent(properties, "catalog", catalog);

    return properties;
  }

  static void setIfPresent(Properties properties, String name, String value) {
    if (value != null) {
      properties.setProperty(name, value);
    }
  }
}
