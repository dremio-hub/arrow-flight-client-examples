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

import java.util.Map;
import java.util.Optional;

final class ExampleEnvironment {
  private final Map<String, String> variables;

  private ExampleEnvironment(Map<String, String> variables) {
    this.variables = variables;
  }

  static ExampleEnvironment system() {
    return new ExampleEnvironment(System.getenv());
  }

  static ExampleEnvironment of(Map<String, String> variables) {
    return new ExampleEnvironment(variables);
  }

  String require(String name) {
    return optional(name)
        .orElseThrow(() -> new IllegalArgumentException(
            "Missing required environment variable: " + name));
  }

  Optional<String> optional(String name) {
    final String value = variables.get(name);
    if (value == null || value.trim().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  int getInt(String name, int defaultValue) {
    final Optional<String> value = optional(name);
    if (!value.isPresent()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.get());
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(
          "Environment variable " + name + " must be an integer.", ex);
    }
  }

  boolean getBoolean(String name, boolean defaultValue) {
    final Optional<String> value = optional(name);
    if (!value.isPresent()) {
      return defaultValue;
    }

    if ("true".equalsIgnoreCase(value.get())) {
      return true;
    }
    if ("false".equalsIgnoreCase(value.get())) {
      return false;
    }

    throw new IllegalArgumentException(
        "Environment variable " + name + " must be true or false.");
  }
}
