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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;

/**
 * Configuration manager that handles configuration from multiple sources with priority:
 * 1. Environment variables (highest priority)
 * 2. Properties file
 * 3. Command line arguments
 * 4. Default values (lowest priority)
 */
public class ConfigurationManager {
  
  // Environment variable prefix for flight client settings
  private static final String ENV_PREFIX = "FLIGHT_";
  
  // Default properties file name
  private static final String DEFAULT_PROPERTIES_FILE = "flight-client.properties";
  
  private final ConfigurationData config;
  private final Properties propertiesFile;
  private final Map<String, String> environmentVariables;

  /**
   * Creates a new ConfigurationManager instance.
   */
  public ConfigurationManager() {
    this.config = new ConfigurationData();
    this.propertiesFile = new Properties();
    this.environmentVariables = loadEnvironmentVariables();
  }
  
  /**
   * Parse configuration from all sources with the specified priority order.
   * 
   * @param args command line arguments
   * @param propertiesFilePath optional path to properties file (null to use default)
   * @return populated ConfigurationData object
   * @throws ConfigurationException if there are issues parsing configuration
   */
  public ConfigurationData parseConfiguration(String[] args, String propertiesFilePath) 
      throws ConfigurationException {
    
    // Step 1: Load properties file (if specified or default exists)
    loadPropertiesFile(propertiesFilePath);
    
    // Step 2: Parse command line arguments
    parseCommandLineArguments(args);
    
    // Step 3: Apply configuration in priority order (env vars override everything)
    applyConfigurationWithPriority();
    
    return config;
  }
  
  /**
   * Parse configuration using default properties file location.
   */
  public ConfigurationData parseConfiguration(String[] args) throws ConfigurationException {
    return parseConfiguration(args, null);
  }
  
  /**
   * Load environment variables with the FLIGHT_ prefix.
   */
  private Map<String, String> loadEnvironmentVariables() {
    Map<String, String> envVars = new HashMap<>();
    System.getenv().forEach((key, value) -> {
      if (key.startsWith(ENV_PREFIX)) {
        String configKey = key.substring(ENV_PREFIX.length()).toLowerCase();
        envVars.put(configKey, value);
      }
    });
    return envVars;
  }
  
  /**
   * Load properties from file.
   */
  private void loadPropertiesFile(String propertiesFilePath) throws ConfigurationException {
    String filePath = propertiesFilePath != null ? propertiesFilePath : DEFAULT_PROPERTIES_FILE;
    
    try (InputStream input = new FileInputStream(filePath)) {
      propertiesFile.load(input);
    } catch (IOException e) {
      // If no custom path specified and default doesn't exist, that's OK
      if (propertiesFilePath != null) {
        throw new ConfigurationException("Failed to load properties file: " + filePath, e);
      }
      // Try to load from classpath
      try (InputStream input = getClass().getClassLoader().getResourceAsStream(DEFAULT_PROPERTIES_FILE)) {
        if (input != null) {
          propertiesFile.load(input);
        }
      } catch (IOException ex) {
        // Ignore - properties file is optional
      }
    }
  }
  
  /**
   * Parse command line arguments using JCommander.
   */
  private void parseCommandLineArguments(String[] args) throws ConfigurationException {
    JCommander jCommander = new JCommander(config);
    jCommander.setProgramName("Java Flight Client");
    
    try {
      jCommander.parse(args);
    } catch (ParameterException e) {
      throw new ConfigurationException("Failed to parse command line arguments: " + e.getMessage(), e);
    }
    
    if (config.help) {
      jCommander.usage();
      System.exit(0);
    }
  }
  
  /**
   * Apply configuration values in priority order.
   * Priority: 1) env vars, 2) properties file, 3) command line, 4) defaults
   */
  private void applyConfigurationWithPriority() {
    // Host
    config.host = getConfigValue("host", config.host, "localhost");
    
    // Port
    String portStr = getConfigValue("port", String.valueOf(config.port), "32010");
    config.port = Integer.parseInt(portStr);
    
    // Username
    config.user = getConfigValue("user", config.user, null);
    
    // Password
    config.pass = getConfigValue("pass", config.pass, null);
    
    // Personal Access Token
    config.patOrAuthToken = getConfigValue("pat", config.patOrAuthToken, null);
    
    // Query
    config.query = getConfigValue("query", config.query, null);
    
    // Binary path
    config.pathToSaveQueryResultsTo = getConfigValue("binpath", config.pathToSaveQueryResultsTo, null);
    
    // TLS settings
    String tlsStr = getConfigValue("tls", String.valueOf(config.enableTls), "false");
    config.enableTls = Boolean.parseBoolean(tlsStr);
    
    String dsvStr = getConfigValue("dsv", String.valueOf(config.disableServerVerification), "false");
    config.disableServerVerification = Boolean.parseBoolean(dsvStr);
    
    // Keystore settings
    config.keystorePath = getConfigValue("keystorepath", config.keystorePath, null);
    config.keystorePass = getConfigValue("keystorepass", config.keystorePass, null);
    
    // Demo flag
    String demoStr = getConfigValue("demo", String.valueOf(config.runDemo), "false");
    config.runDemo = Boolean.parseBoolean(demoStr);
    
    // Engine
    config.engine = getConfigValue("engine", config.engine, null);
    
    // Project ID
    config.projectId = getConfigValue("projectid", config.projectId, null);
  }
  
  /**
   * Get configuration value with priority order.
   * 1. Environment variable (highest priority)
   * 2. Properties file
   * 3. Command line argument
   * 4. Default value (lowest priority)
   */
  private String getConfigValue(String key, String cmdLineValue, String defaultValue) {
    // 1. Check environment variables first
    String envValue = environmentVariables.get(key);
    if (!Strings.isNullOrEmpty(envValue)) {
      return envValue;
    }
    
    // 2. Check properties file
    String propValue = propertiesFile.getProperty(key);
    if (!Strings.isNullOrEmpty(propValue)) {
      return propValue;
    }
    
    // 3. Use command line value if provided
    if (!Strings.isNullOrEmpty(cmdLineValue)) {
      return cmdLineValue;
    }
    
    // 4. Fall back to default
    return defaultValue;
  }
  
  /**
   * Get the loaded configuration data.
   */
  public ConfigurationData getConfiguration() {
    return config;
  }
  
  /**
   * Print configuration sources for debugging.
   */
  public void printConfigurationSources() {
    System.out.println("Configuration Sources:");
    System.out.println("Environment variables: " + environmentVariables.size() + " found");
    System.out.println("Properties file entries: " + propertiesFile.size() + " found");
    System.out.println("Command line arguments: parsed successfully");
  }
}
