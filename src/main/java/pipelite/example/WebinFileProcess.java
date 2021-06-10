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
package pipelite.example;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;
import pipelite.Pipeline;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

@Component
public class WebinFileProcess implements Pipeline {
  private final String pipelineName;
  private final int pipelineParallelism;
  private final AtomicInteger cnt = new AtomicInteger(1);

  public WebinFileProcess() {
    pipelineName = "WEBIN_FILE_PROCESS_RASKO";
    pipelineParallelism = 1;
  }

  @Override
  public String pipelineName() {
    return pipelineName;
  }

  @Override
  public Options configurePipeline() {
    return new Options().pipelineParallelism(pipelineParallelism);
  }

  private static final SimpleLsfExecutorParameters STAGE_PARAMS =
      SimpleLsfExecutorParameters.builder()
          .immediateRetries(2)
          .maximumRetries(5)
          .cpu(1)
          .timeout(Duration.ofDays(1))
          .permanentError(60) // USER_ERROR("USER_ERROR", 60, UserErrorException.class,
          // RESULT_TYPE.PERMANENT_ERROR)
          .permanentError(70) // SYSTEM_ERROR("SYSTEM_ERROR", 70, SystemErrorException.class,
          // RESULT_TYPE.PERMANENT_ERROR)
          .user("era")
          .host("hh-sra-01-02.ebi.ac.uk")
          .queue("production")
          .workDir("/.firefs/cache/era/webin-file-process-workdir")
          .build();

  @Override
  public void configureProcess(ProcessBuilder builder) {
    builder
        .execute("TEST_PERMANENT_ERROR")
        .withSimpleLsfExecutor("/homes/era/return_60.sh", STAGE_PARAMS);
  }

  @Override
  public Process nextProcess() {
    if (cnt.getAndDecrement() == 1) {
      return new Process(UUID.randomUUID().toString());
    }
    return null;
  }

  @Override
  public void confirmProcess(String processId) {}
}
