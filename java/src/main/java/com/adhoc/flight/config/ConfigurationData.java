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

package com.adhoc.flight.config;

import java.util.ArrayList;
import java.util.List;

import com.adhoc.flight.SessionProperty;
import com.adhoc.flight.SessionPropertyConverter;
import com.beust.jcommander.Parameter;

/**
 * Configuration data class that holds all the configuration parameters
 * for the Flight client application. This class uses JCommander annotations
 * for command line parsing but can also be populated from other sources.
 */
public class ConfigurationData {
  
  @Parameter(names = {"-host", "--hostname"},
      description = "Dremio co-ordinator hostname. Defaults to \"localhost\".")
  public String host = "localhost";

  @Parameter(names = {"-port", "--flightport"},
      description = "Dremio flight server port. Defaults to 32010.")
  public int port = 32010;

  @Parameter(names = {"-user", "--username"},
      description = "Dremio username. Defaults to \"dremio\".")
  public String user;

  @Parameter(names = {"-pass", "--password"},
      description = "Dremio password. Defaults to \"dremio123\".")
  public String pass;

  @Parameter(names = {"-pat", "--personalAccessToken", "-authToken", "--authToken"},
      description = "Either a Personal Access Token or an OAuth2 Token.")
  public String patOrAuthToken;

  @Parameter(names = {"-query", "--sqlQuery"},
      description = "SQL query to test.")
  public String query = null;

  @Parameter(names = {"-binpath", "--saveBinaryPath"},
      description = "Path to save the SQL result binary to.")
  public String pathToSaveQueryResultsTo = null;

  @Parameter(names = {"-tls", "--tls"},
      description = "Enable encrypted connection. Defaults to false.")
  public boolean enableTls = false;

  @Parameter(names = {"-dsv", "--disableServerVerification"},
      description = "Disable TLS server verification. Defaults to false.")
  public boolean disableServerVerification = false;

  @Parameter(names = {"-kstpath", "--keyStorePath"},
      description = "Path to the jks keystore. Defaults to system Keystore.")
  public String keystorePath = null;

  @Parameter(names = {"-kstpass", "--keyStorePassword"},
      description = "The jks keystore password.")
  public String keystorePass = null;

  @Parameter(names = {"-demo", "--runDemo"},
      description = "A flag to to run a demo of querying the Dremio Flight Server Endpoint. Defaults to false.")
  public boolean runDemo = false;

  @Parameter(names = {"-engine", "--engine"},
      description = "The specific engine to run against.")
  public String engine;

  @Parameter(names = {"-projectId", "--projectId"},
      description = "Dremio Cloud project to connect to.")
  public String projectId;

  @Parameter(names = {"-sp", "--sessionProperty"},
      description = "Key value pairs of SessionProperty, " +
        "example: -sp schema='Samples.\"samples.dremio.com\"' -sp key=value",
      listConverter = SessionPropertyConverter.class)
  public List<SessionProperty> sessionProperties = new ArrayList<>();

  @Parameter(names = {"-h", "--help"},
      description = "Show usage.", help = true)
  public boolean help = false;

  @Parameter(names = {"-config", "--configFile"},
      description = "Path to properties configuration file.")
  public String configFile = null;

  @Parameter(names = {"-v", "--verbose"},
      description = "Enable verbose output including configuration sources.")
  public boolean verbose = false;
  
  /**
   * Create a copy of this configuration data.
   */
  public ConfigurationData copy() {
    ConfigurationData copy = new ConfigurationData();
    copy.host = this.host;
    copy.port = this.port;
    copy.user = this.user;
    copy.pass = this.pass;
    copy.patOrAuthToken = this.patOrAuthToken;
    copy.query = this.query;
    copy.pathToSaveQueryResultsTo = this.pathToSaveQueryResultsTo;
    copy.enableTls = this.enableTls;
    copy.disableServerVerification = this.disableServerVerification;
    copy.keystorePath = this.keystorePath;
    copy.keystorePass = this.keystorePass;
    copy.runDemo = this.runDemo;
    copy.engine = this.engine;
    copy.projectId = this.projectId;
    copy.sessionProperties = new ArrayList<>(this.sessionProperties);
    copy.help = this.help;
    copy.configFile = this.configFile;
    copy.verbose = this.verbose;
    return copy;
  }
  
  /**
   * Print current configuration values (excluding sensitive data like passwords).
   */
  public void printConfiguration() {
    System.out.println("Current Configuration:");
    System.out.println("  Host: " + host);
    System.out.println("  Port: " + port);
    System.out.println("  User: " + user);
    System.out.println("  Password: " + (pass != null ? "***" : "null"));
    System.out.println("  PAT/Auth Token: " + (patOrAuthToken != null ? "***" : "null"));
    System.out.println("  Query: " + query);
    System.out.println("  Binary Path: " + pathToSaveQueryResultsTo);
    System.out.println("  TLS Enabled: " + enableTls);
    System.out.println("  Disable Server Verification: " + disableServerVerification);
    System.out.println("  Keystore Path: " + keystorePath);
    System.out.println("  Keystore Password: " + (keystorePass != null ? "***" : "null"));
    System.out.println("  Run Demo: " + runDemo);
    System.out.println("  Engine: " + engine);
    System.out.println("  Project ID: " + projectId);
    System.out.println("  Session Properties: " + sessionProperties.size() + " entries");
    System.out.println("  Config File: " + configFile);
    System.out.println("  Verbose: " + verbose);
  }
  
  /**
   * Validate the configuration and throw an exception if invalid.
   */
  public void validate() throws ConfigurationException {
    if (port <= 0 || port > 65535) {
      throw new ConfigurationException("Port must be between 1 and 65535, got: " + port);
    }
    
    if (host == null || host.trim().isEmpty()) {
      throw new ConfigurationException("Host cannot be null or empty");
    }
    
    if (!runDemo && (query == null || query.trim().isEmpty())) {
      throw new ConfigurationException("Query is required when not running demo mode");
    }
    
    if (enableTls && keystorePath != null && (keystorePass == null || keystorePass.trim().isEmpty())) {
      throw new ConfigurationException("Keystore password is required when keystore path is specified");
    }
  }
}
