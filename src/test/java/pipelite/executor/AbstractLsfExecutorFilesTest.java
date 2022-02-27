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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.AbstractLsfExecutorParameters;
import pipelite.stage.path.LsfFilePathResolver;

public class AbstractLsfExecutorFilesTest {

  private static class TestLsfExecutor extends AbstractLsfExecutor<AbstractLsfExecutorParameters> {
    @Override
    public String getSubmitCmd(StageExecutorRequest request) {
      throw new PipeliteException("");
    }
  }

  @Test
  public void resolveDefaultLogDir() {
    TestLsfExecutor executor = new TestLsfExecutor();
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName("PIPELINE_NAME")
            .processId("PROCESS_ID")
            .stage(stage)
            .build();
    AbstractLsfExecutorParameters params = AbstractLsfExecutorParameters.builder().build();

    assertThat(params.resolveLogDir(request, LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("%U/PIPELINE_NAME/PROCESS_ID");

    params = AbstractLsfExecutorParameters.builder().user("user").build();

    assertThat(params.resolveLogDir(request, LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("user/PIPELINE_NAME/PROCESS_ID");
  }

  @Test
  public void resolveDefaultLogFile() {
    TestLsfExecutor executor = new TestLsfExecutor();
    Stage stage = Stage.builder().stageName("STAGE_NAME").executor(executor).build();
    StageExecutorRequest request =
        StageExecutorRequest.builder()
            .pipelineName("PIPELINE_NAME")
            .processId("PROCESS_ID")
            .stage(stage)
            .build();
    AbstractLsfExecutorParameters params = AbstractLsfExecutorParameters.builder().build();

    assertThat(params.resolveLogFile(request, LsfFilePathResolver.Format.WITH_LSF_PATTERN))
        .isEqualTo("%U/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out");

    params = AbstractLsfExecutorParameters.builder().user("user").build();

    assertThat(params.resolveLogFile(request, LsfFilePathResolver.Format.WITHOUT_LSF_PATTERN))
        .isEqualTo("user/PIPELINE_NAME/PROCESS_ID/STAGE_NAME.out");
  }
}
