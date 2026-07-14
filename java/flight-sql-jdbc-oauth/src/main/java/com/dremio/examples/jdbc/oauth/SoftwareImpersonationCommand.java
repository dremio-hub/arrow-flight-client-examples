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
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import java.util.Properties;

@Parameters(commandNames = "software-impersonation",
    commandDescription = "Dremio Software inbound impersonation with basic auth "
        + "(Software > 26.1.9 only)")
final class SoftwareImpersonationCommand {
  static final String DEFAULT_VALIDATION_QUERY = "SELECT USER() AS user_name, "
      + "\"SESSION_USER\"() AS session_user_name, "
      + "\"SYSTEM_USER\"() AS system_user_name";

  @ParametersDelegate
  ConnectionParams connection = new ConnectionParams(DEFAULT_VALIDATION_QUERY);

  @Parameter(names = {"--username", "--proxy-user"},
      description = "Dremio proxy username. Env: DREMIO_USERNAME")
  String username = ConnectionParams.stringEnv("DREMIO_USERNAME", "dremio");

  @Parameter(names = "--password", password = true,
      description = "Dremio password for the proxy user. Env: DREMIO_PASSWORD")
  String password = ConnectionParams.stringEnv("DREMIO_PASSWORD", null);

  @Parameter(names = "--target-user",
      description = "Dremio target user to impersonate. Env: DREMIO_TARGET_USER")
  String targetUser = ConnectionParams.stringEnv("DREMIO_TARGET_USER", null);

  Properties toProperties() {
    if (password == null || password.isEmpty()) {
      throw new ParameterException("--password or DREMIO_PASSWORD is required.");
    }
    if (targetUser == null || targetUser.isEmpty()) {
      throw new ParameterException("--target-user or DREMIO_TARGET_USER is required.");
    }

    final Properties properties = connection.toProperties();
    properties.setProperty("user", username);
    properties.setProperty("password", password);
    properties.setProperty("impersonation_target", targetUser);
    return properties;
  }
}
