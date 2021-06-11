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
package pipelite.helper;

import java.time.Duration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public abstract class SingleStageSimpleLsfScheduleTestHelper extends ScheduleTestHelper {

  private final String cmd;
  private final int exitCode;
  private final int immediateRetries;
  private final int maximumRetries;
  private final LsfTestConfiguration lsfTestConfiguration;
  private final String stageName = "STAGE";
  private SimpleLsfExecutorParameters executorParams;

  public SingleStageSimpleLsfScheduleTestHelper(
      String cron,
      int exitCode,
      int immediateRetries,
      int maximumRetries,
      LsfTestConfiguration lsfTestConfiguration) {
    super(cron);
    this.cmd = "bash -c 'exit '" + exitCode;
    this.exitCode = exitCode;
    this.immediateRetries = immediateRetries;
    this.maximumRetries = maximumRetries;
    this.lsfTestConfiguration = lsfTestConfiguration;
  }

  @Override
  public void testConfigureProcess(ProcessBuilder builder) {
    SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?> executorParamsBuilder =
        SimpleLsfExecutorParameters.builder();
    executorParamsBuilder
        .host(lsfTestConfiguration.getHost())
        .workDir(lsfTestConfiguration.getWorkDir())
        .timeout(Duration.ofSeconds(180))
        .maximumRetries(maximumRetries)
        .immediateRetries(immediateRetries);
    testExecutorParams(executorParamsBuilder);
    executorParams = executorParamsBuilder.build();
    builder.execute(stageName).withSimpleLsfExecutor(cmd, executorParams);
  }

  protected void testExecutorParams(
      SimpleLsfExecutorParameters.SimpleLsfExecutorParametersBuilder<?, ?> executorParamsBuilder) {}

  public String cmd() {
    return cmd;
  }

  public int exitCode() {
    return exitCode;
  }

  public String stageName() {
    return stageName;
  }

  public SimpleLsfExecutorParameters executorParams() {
    return executorParams;
  }
}
