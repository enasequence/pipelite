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
package pipelite.example.launcher;

import java.time.Duration;
import org.springframework.stereotype.Component;
import pipelite.executor.StageExecutorParameters;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.builder.ProcessBuilder;

@Component
public class LsfSshExampleProcessFactory implements ProcessFactory {

  public static final String PIPELINE_NAME = "lsfSshExample";

  @Override
  public String getPipelineName() {
    return PIPELINE_NAME;
  }

  private static final String SUCCESS_CMD = "sh -c 'exit 0'";
  private static final String FAILURE_CMD = "sh -c 'exit 1'";

  private static final StageExecutorParameters STAGE_PARAMS =
      StageExecutorParameters.builder()
          .immediateRetries(2)
          .maximumRetries(10)
          .cores(1)
          .memory(16 /* MBytes */)
          .timeout(Duration.ofMinutes(5))
          .build();

  @Override
  public Process create(String processId) {
    return new ProcessBuilder(processId)
        .execute("STAGE1", STAGE_PARAMS)
        .withLsfSshCmdExecutor(SUCCESS_CMD)
        .executeAfterPrevious("STAGE2", STAGE_PARAMS)
        .withLsfSshCmdExecutor(SUCCESS_CMD)
        .build();
  }
}
