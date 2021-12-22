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

package com.adhoc.flight;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Tests for {@link QueryRunner}. Checking parsing functions.
 */
public class TestQueryRunner {
  @Parameter(names = "--sessionProperties", variableArity = true, listConverter = SessionPropertyConverter.class)
  List<SessionProperty> sessionProperties;

  @Test
  public void testParseSessionProperties() {
    JCommander jc = new JCommander(this);
    jc.parse("--sessionProperties", "key1:value1", "key2:value2");
    Assert.assertNotNull(sessionProperties);
    Assert.assertEquals(2, sessionProperties.size());
    Assert.assertEquals("key1", sessionProperties.get(0).getKey());
    Assert.assertEquals("value1", sessionProperties.get(0).getValue());
    Assert.assertEquals("key2", sessionProperties.get(1).getKey());
    Assert.assertEquals("value2", sessionProperties.get(1).getValue());
  }
}
