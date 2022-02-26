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
package pipelite.runner.stage;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.Pipeline;
import pipelite.PipeliteTestConfigWithManager;
import pipelite.UniqueStringGenerator;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.executor.SimpleLsfExecutor;
import pipelite.executor.state.AsyncExecutorState;
import pipelite.metrics.PipeliteMetrics;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.runner.process.ProcessRunner;
import pipelite.runner.process.creator.ProcessEntityCreator;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;

@SpringBootTest(
    classes = PipeliteTestConfigWithManager.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=ScheduleRunnerTest",
      "pipelite.advanced.processRunnerFrequency=250ms",
      "pipelite.advanced.shutdownIfIdle=true"
    })
@ActiveProfiles({"test"})
@DirtiesContext
public class StageRunnerSimpleLsfExecutorTest {

  @Autowired private PipeliteServices pipeliteServices;
  @Autowired private PipeliteMetrics pipeliteMetrics;
  @Autowired private ExecutorConfiguration executorConfiguration;
  @Autowired private LsfTestConfiguration lsfTestConfiguration;
  private final String serviceName = UniqueStringGenerator.randomServiceName();
  private final String pipelineName = UniqueStringGenerator.randomPipelineName();
  private final String stageName = UniqueStringGenerator.randomStageName();

  private StageRunner stageRunner(SimpleLsfExecutor executor) {
    // Create executor parameters
    SimpleLsfExecutorParameters params =
        SimpleLsfExecutorParameters.builder()
            .host(lsfTestConfiguration.getHost())
            .workDir(lsfTestConfiguration.getWorkDir())
            .timeout(Duration.ofSeconds(30))
            .logSave(LogFileSavePolicy.ALWAYS)
            .build();

    // Create executor
    executor.setCmd("echo test");
    executor.setExecutorParams(params);

    // Create process
    ProcessBuilder processBuilder = new ProcessBuilder(UniqueStringGenerator.randomProcessId());
    processBuilder.execute(stageName).with(executor, params);
    Process process = processBuilder.build();

    // Create process entity
    process.setProcessEntity(
        ProcessEntityCreator.create(
            pipeliteServices.process(),
            pipelineName,
            new Pipeline.Process(process.getProcessId())));

    // Start process execution
    ProcessRunner.startProcessExecution(
        pipeliteServices, executorConfiguration, pipelineName, process);

    // Create stage runner
    return new StageRunner(
        pipeliteServices,
        pipeliteMetrics,
        serviceName,
        pipelineName,
        process,
        process.getStage(stageName).get());
  }

  private class SubmitSubmittedExecutor extends SimpleLsfExecutor {
    @Override
    public StageExecutorResult submit(StageExecutorRequest request) {
      setJobId(UniqueStringGenerator.id());
      return StageExecutorResult.submitted();
    }
  }

  private class SubmitExceptionExecutor extends SimpleLsfExecutor {
    @Override
    public StageExecutorResult submit(StageExecutorRequest request) {
      throw new RuntimeException("Excepted exception");
    }
  }

  private class SubmitErrorExecutor extends SimpleLsfExecutor {
    @Override
    public StageExecutorResult submit(StageExecutorRequest request) {
      return StageExecutorResult.error();
    }
  }

  @Test
  public void submitSubmitted() {
    StageRunner stageRunner = stageRunner(new SubmitSubmittedExecutor());

    Process process = stageRunner.getProcess();
    Stage stage = process.getStage(stageName).get();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);

    stageRunner.runOneIteration(r -> {});

    StageExecutorResult result = stageRunner.getExecutorResult();
    assertThat(result.isSubmitted()).isTrue();
    assertThat(result.getExecutorState()).isEqualTo(StageExecutorState.SUBMITTED);
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);
  }

  @Test
  public void submitException() {
    StageRunner stageRunner = stageRunner(new SubmitExceptionExecutor());

    Process process = stageRunner.getProcess();
    Stage stage = process.getStage(stageName).get();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(result.get().getExecutorState()).isEqualTo(StageExecutorState.ERROR);
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ERROR);
  }

  @Test
  public void submitError() {
    StageRunner stageRunner = stageRunner(new SubmitErrorExecutor());

    Process process = stageRunner.getProcess();
    Stage stage = process.getStage(stageName).get();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);

    AtomicReference<StageExecutorResult> result = new AtomicReference<>();
    stageRunner.runOneIteration(r -> result.set(r));

    assertThat(result.get().isError()).isTrue();
    assertThat(result.get().isErrorType(ErrorType.INTERNAL_ERROR)).isFalse();
    assertThat(result.get().getExecutorState()).isEqualTo(StageExecutorState.ERROR);
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ERROR);
  }

  @Test
  public void pollMissingJobid() {
    SubmitSubmittedExecutor executor = new SubmitSubmittedExecutor();
    StageRunner stageRunner = stageRunner(executor);

    Process process = stageRunner.getProcess();
    Stage stage = process.getStage(stageName).get();
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);

    stageRunner.runOneIteration(r -> {});

    StageExecutorResult result = stageRunner.getExecutorResult();
    assertThat(result.isSubmitted()).isTrue();
    assertThat(result.getExecutorState()).isEqualTo(StageExecutorState.SUBMITTED);
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(((SimpleLsfExecutor) stage.getExecutor()).getState())
        .isEqualTo(AsyncExecutorState.POLL);

    executor.setJobId(null);

    stageRunner.runOneIteration(r -> {});

    result = stageRunner.getExecutorResult();
    assertThat(result.isError()).isTrue();
    assertThat(result.isErrorType(ErrorType.INTERNAL_ERROR)).isTrue();
    assertThat(result.getExecutorState()).isEqualTo(StageExecutorState.ERROR);
    assertThat(stage.getStageEntity().getStageState()).isEqualTo(StageState.ERROR);
    assertThat(((SimpleLsfExecutor) stage.getExecutor()).getState())
        .isEqualTo(AsyncExecutorState.POLL);
    assertThat(result.getStageLog()).contains("Missing job id during asynchronous poll");
  }
}
