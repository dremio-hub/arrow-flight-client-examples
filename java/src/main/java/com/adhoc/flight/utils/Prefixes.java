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

package com.adhoc.flight.utils;

import org.apache.arrow.util.Preconditions;

/**
 * Hold some prefixes used when printing results.
 */
enum Prefixes {
  ERROR("[ERROR]"),
  INFORMATION("[INFO]");

  private final String string;

  Prefixes(String string) {
    Preconditions.checkArgument(string.length() > 0);
    this.string = string;
  }

  /**
   * Get the representation of this instace, formatted as String.
   *
   * @return the {@code String} representation of this instance, formatted.
   */
  public String toFormattedString() {
    return string;
  }
}
