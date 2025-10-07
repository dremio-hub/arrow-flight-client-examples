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

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test class for ConfigurationManager to verify configuration priority order.
 */
public class ConfigurationManagerTest {

  @Test
  public void testDefaultConfiguration() throws ConfigurationException {
    ConfigurationManager manager = new ConfigurationManager();
    ConfigurationData config = manager.parseConfiguration(new String[]{});
    
    // Test default values
    assertEquals("localhost", config.host);
    assertEquals(32010, config.port);
    assertFalse(config.enableTls);
    assertFalse(config.runDemo);
  }

  @Test
  public void testCommandLineOverridesDefaults() throws ConfigurationException {
    ConfigurationManager manager = new ConfigurationManager();
    String[] args = {"--hostname", "test-host", "--port", "9999", "--tls"};
    ConfigurationData config = manager.parseConfiguration(args);
    
    assertEquals("test-host", config.host);
    assertEquals(9999, config.port);
    assertTrue(config.enableTls);
  }

  @Test
  public void testConfigurationValidation() {
    ConfigurationData config = new ConfigurationData();
    config.port = -1; // Invalid port
    
    try {
      config.validate();
      fail("Expected ConfigurationException for invalid port");
    } catch (ConfigurationException e) {
      assertTrue(e.getMessage().contains("Port must be between 1 and 65535"));
    }
  }

  @Test
  public void testConfigurationCopy() {
    ConfigurationData original = new ConfigurationData();
    original.host = "test-host";
    original.port = 9999;
    original.enableTls = true;
    
    ConfigurationData copy = original.copy();
    
    assertEquals(original.host, copy.host);
    assertEquals(original.port, copy.port);
    assertEquals(original.enableTls, copy.enableTls);
    
    // Verify it's a deep copy
    copy.host = "different-host";
    assertNotEquals(original.host, copy.host);
  }

  @Test
  public void testHelpFlag() throws ConfigurationException {
    ConfigurationManager manager = new ConfigurationManager();
    String[] args = {"--help"};
    
    // This should not throw an exception and should set help flag
    ConfigurationData config = manager.parseConfiguration(args);
    assertTrue(config.help);
  }

  @Test
  public void testVerboseFlag() throws ConfigurationException {
    ConfigurationManager manager = new ConfigurationManager();
    String[] args = {"--verbose"};
    ConfigurationData config = manager.parseConfiguration(args);
    
    assertTrue(config.verbose);
  }
}
