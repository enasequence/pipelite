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
package pipelite;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pipelite.executor.AbstractExecutor;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

@Configuration
public class PipeliteTest {

  private static final int PROCESS_CNT = 10000;
  private static final int PROCESS_PARALLELISM = 10;
  private static final Duration STAGE_EXECUTION_TIME = Duration.ofSeconds(5);

  @Bean
  public TestPipeline successPipeline() {
    return new TestPipeline("PIPELINE_SUCCESS_1", new TestExecutor(StageExecutorResult.success()));
  }

  @Bean
  public TestPipeline errorPipeline() {
    return new TestPipeline("PIPELINE_ERROR_1", new TestExecutor(StageExecutorResult.error()));
  }

  @Bean
  public TestSchedule successSchedule() {
    return new TestSchedule(
        "SCHEDULE_SUCCESS_1",
        builder ->
            builder
                .execute("SCHEDULE_STAGE_SUCCESS_1")
                .with(new TestExecutor(StageExecutorResult.success())));
  }

  @Bean
  public TestSchedule errorSchedule() {
    return new TestSchedule(
        "SCHEDULE_ERROR_1",
        builder ->
            builder
                .execute("SCHEDULE_STAGE_ERROR_1")
                .with(new TestExecutor(StageExecutorResult.error())));
  }
  // }

  public static class TestPipeline<T extends TestExecutor>
      extends ConfigurableTestPipeline<TestProcessConfiguration> {

    public TestPipeline(String pipelineName, T stageExecutor) {
      super(
          PROCESS_PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration(pipelineName) {
            @Override
            protected void configure(ProcessBuilder builder) {
              ExecutorParameters executorParams =
                  ExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();
              builder
                  .execute(
                      "PIPELINE_STAGE_" + stageExecutor.result().getExecutorState().name() + "_1")
                  .with(stageExecutor, executorParams);
            }
          });
    }
  }

  public static class TestSchedule implements Schedule {

    private final String pipelineName;
    private final Consumer<ProcessBuilder> configureProcess;

    public TestSchedule(String pipelineName, Consumer<ProcessBuilder> configureProcess) {
      this.pipelineName = pipelineName;
      this.configureProcess = configureProcess;
    }

    @Override
    public String pipelineName() {
      return pipelineName;
    }

    @Override
    public Options configurePipeline() {
      return new Options().cron(PipeliteTestConstants.CRON_EVERY_TWO_SECONDS);
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
      configureProcess.accept(builder);
    }
  }

  public static class TestExecutor extends AbstractExecutor<ExecutorParameters> {
    private final StageExecutorResult result;
    private final Map<String, LocalDateTime> firstExecute = new ConcurrentHashMap<>();

    public TestExecutor(StageExecutorResult result) {
      this.result = result;
    }

    @Override
    public StageExecutorResult execute(StageExecutorRequest request) {
      String processId = request.getProcessId();
      if (!firstExecute.containsKey(processId)) {
        firstExecute.put(processId, LocalDateTime.now());
      }
      if (Duration.between(firstExecute.get(processId), LocalDateTime.now())
              .compareTo(STAGE_EXECUTION_TIME)
          < 0) {
        return StageExecutorResult.active();
      }
      result.setStageLog(
          "Stage execution result "
              + result.getExecutorState().name()
              + " pipeline "
              + request.getPipelineName()
              + " process "
              + request.getProcessId()
              + " stage "
              + request.getStage().getStageName());
      return result;
    }

    public StageExecutorResult result() {
      return result;
    }

    @Override
    public void terminate() {}
  }

  // @Test
  public static void main(String[] args) {
    System.setProperty("pipelite.service.name", "PipeliteTest");
    System.setProperty("pipelite.advanced.processRunnerFrequency", "1s");
    System.setProperty("pipelite.advanced.processQueueMinRefreshFrequency", "10s");
    System.setProperty("pipelite.advanced.processQueueMaxRefreshFrequency", "60s");

    // Make sure we use the in-memory database.
    System.setProperty("pipelite.datasource.driverClassName", "");
    System.setProperty("spring.profiles.active", "test");

    Pipelite.main(new String[0]);
  }
}
