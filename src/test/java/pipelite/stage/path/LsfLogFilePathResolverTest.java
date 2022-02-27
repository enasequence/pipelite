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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;

public class LsfLogFilePathResolverTest {

  private static final String PIPELITE_NAME = "TEST_PIPELINE";
  private static final String PROCESS_ID = "TEST_PROCESS";
  private static final String STAGE_NAME = "TEST_STAGE";

  private final StageExecutorRequest request() {
    Stage stage = Mockito.mock(Stage.class);
    when(stage.getStageName()).thenReturn(STAGE_NAME);
    StageExecutorRequest request = new StageExecutorRequest(PIPELITE_NAME, PROCESS_ID, stage);
    return request;
  }

  @Test
  public void testWithoutDirWithUser() {
    StageExecutorRequest request = request();
    AbstractLsfExecutorParameters params =
        AbstractLsfExecutorParameters.builder().user("user").build();
    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver(request, params);
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("user/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("user/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithoutDirWithoutUser() {
    StageExecutorRequest request = request();
    AbstractLsfExecutorParameters params = AbstractLsfExecutorParameters.builder().build();
    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver(request, params);
    String user = System.getProperty("user.name");
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo(user + "/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo(user + "/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithDirWithUser() {
    StageExecutorRequest request = request();
    AbstractLsfExecutorParameters params =
        AbstractLsfExecutorParameters.builder().logDir("a/b").user("user").build();
    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver(request, params);
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("a/b/user/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("a/b/user/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithDirWithoutUser() {
    StageExecutorRequest request = request();
    AbstractLsfExecutorParameters params =
        AbstractLsfExecutorParameters.builder().logDir("a/b").build();
    LsfLogFilePathResolver resolver = new LsfLogFilePathResolver(request, params);
    String user = System.getProperty("user.name");
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("a/b/" + user + "/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo(
            "a/b/" + user + "/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
    assertThat(resolver.getDir(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID);
    assertThat(resolver.getFile(LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("a/b/%U/" + PIPELITE_NAME + "/" + PROCESS_ID + "/" + STAGE_NAME + ".out");
  }
}
