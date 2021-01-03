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

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.stage.Stage;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleLsfExecutorArgumentTest {

  private final String PIPELINE_NAME = UniqueStringGenerator.randomPipelineName();
  private final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  private static SimpleLsfExecutor createExecutor(SimpleLsfExecutorParameters params) {
    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    executor.setCmd("");
    executor.setExecutorParams(params);
    return executor;
  }

  private Stage createStage(SimpleLsfExecutor executor) {
    return Stage.builder()
        .stageName(UniqueStringGenerator.randomStageName())
        .executor(executor)
        .build();
  }

  @Test
  public void memoryAndMemoryDurationAndCpuAndQueueAndWorkDir() throws IOException {
    SimpleLsfExecutorParameters executorParams =
        SimpleLsfExecutorParameters.builder()
            .workDir(Files.createTempDirectory("TEMP").toString())
            .cpu(2)
            .memory(1)
            .memoryTimeout(Duration.ofMinutes(1))
            .queue("TEST")
            .build();

    SimpleLsfExecutor executor = createExecutor(executorParams);
    String cmd = executor.getFullCmd(PIPELINE_NAME, PROCESS_ID, createStage(executor));
    assertTrue(cmd.contains(" -M 1M -R \"rusage[mem=1M:duration=1]\""));
    assertTrue(cmd.contains(" -n 2"));
    assertTrue(cmd.contains(" -q TEST"));
    assertTrue(cmd.contains(" -oo " + executorParams.getWorkDir()));
  }
}
