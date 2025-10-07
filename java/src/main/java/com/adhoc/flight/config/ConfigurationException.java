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

/**
 * Exception thrown when there are configuration-related errors.
 */
public class ConfigurationException extends Exception {
  
  private static final long serialVersionUID = 1L;
  
  /**
   * Constructs a new configuration exception with the specified detail message.
   *
   * @param message the detail message
   */
  public ConfigurationException(String message) {
    super(message);
  }
  
  /**
   * Constructs a new configuration exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public ConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
  
  /**
   * Constructs a new configuration exception with the specified cause.
   *
   * @param cause the cause
   */
  public ConfigurationException(Throwable cause) {
    super(cause);
  }
}
