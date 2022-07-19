/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.stage.path;

import org.junit.jupiter.api.Test;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;

import static org.assertj.core.api.Assertions.assertThat;

public class LsfLogFilePathResolverTest {

  private static final String PIPELITE_NAME = "TEST_PIPELINE";
  private static final String PROCESS_ID = "TEST_PROCESS";
  private static final String STAGE_NAME = "TEST_STAGE";

  private final StageExecutorRequest request(String user, String logDir) {
    return FilePathResolverTestHelper.request(PIPELITE_NAME, PROCESS_ID, STAGE_NAME, user, logDir);
  }

  @Test
  public void testWithoutDirWithUser() {
    StageExecutorRequest request = request("user", null);

    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver();
    assertThat(resolver.resolvedPath().dir(request))
        .isEqualTo("user/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.resolvedPath().file(request))
        .isEqualTo("user/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.placeholderPath().dir(request))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.placeholderPath().file(request))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithoutDirWithoutUser() {
    StageExecutorRequest request = request(null, null);

    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver();

    String user = System.getProperty("user.name");
    assertThat(resolver.resolvedPath().dir(request))
        .isEqualTo(user + "/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.resolvedPath().file(request))
        .isEqualTo(user + "/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.placeholderPath().dir(request))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.placeholderPath().file(request))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithDirWithUser() {
    StageExecutorRequest request = request("user", "a/b");

    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver();
    assertThat(resolver.resolvedPath().dir(request))
        .isEqualTo("a/b/user/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.resolvedPath().file(request))
        .isEqualTo("a/b/user/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.placeholderPath().dir(request))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.placeholderPath().file(request))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithDirWithoutUser() {
    StageExecutorRequest request = request(null, "a/b");

    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver();
    String user = System.getProperty("user.name");
    assertThat(resolver.resolvedPath().dir(request))
        .isEqualTo("a/b/" + user + "/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.resolvedPath().file(request))
        .isEqualTo(
            "a/b/" + user + "/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.placeholderPath().dir(request))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.placeholderPath().file(request))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }
}
