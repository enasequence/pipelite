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
package pipelite.stage.parameters.cmd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.CmdExecutorParameters;

public class LogFileResolverTest {

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
  public void testDefaultWorkDir() {
    StageExecutorRequest request = request();
    CmdExecutorParameters params = CmdExecutorParameters.builder().build();
    assertThat(LogFileResolver.resolve(request, params))
        .isEqualTo("pipelite" + "/" + PIPELITE_NAME + "_" + PROCESS_ID + "_" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithoutSubstitutionsWorkDir() {
    StageExecutorRequest request = request();
    CmdExecutorParameters params = CmdExecutorParameters.builder().workDir("a/b/c/d").build();
    assertThat(LogFileResolver.resolve(request, params))
        .isEqualTo("a/b/c/d/" + PIPELITE_NAME + "_" + PROCESS_ID + "_" + STAGE_NAME + ".out");
  }

  @Test
  public void testWithSubstitutionsWorkDir() {
    StageExecutorRequest request = request();
    CmdExecutorParameters params =
        CmdExecutorParameters.builder().workDir("a/%PIPELINE%/%PROCESS%/%STAGE%").build();
    assertThat(LogFileResolver.resolve(request, params))
        .isEqualTo(
            "a/"
                + PIPELITE_NAME
                + "/"
                + PROCESS_ID
                + "/"
                + STAGE_NAME
                + "/"
                + PIPELITE_NAME
                + "_"
                + PROCESS_ID
                + "_"
                + STAGE_NAME
                + ".out");
  }
}
