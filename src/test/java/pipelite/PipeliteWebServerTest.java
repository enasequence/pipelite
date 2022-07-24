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
import java.util.function.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

@Configuration
public class PipeliteWebServerTest {

  private static final int PROCESS_CNT = 250;
  private static final int PROCESS_PARALLELISM = 50;
  private static final Duration SUBMIT_TIME = Duration.ofSeconds(45);
  private static final Duration EXECUTION_TIME = Duration.ofSeconds(65);
  private static final String ID = PipeliteIdCreator.id();

  @Bean
  public TestPipeline successPipeline1() {
    return new TestPipeline("PIPELINE_SUCCESS_1_" + ID, StageExecutorState.SUCCESS);
  }

  @Bean
  public TestPipeline successPipeline2() {
    return new TestPipeline("PIPELINE_SUCCESS_2_" + ID, StageExecutorState.SUCCESS);
  }

  @Bean
  public TestPipeline successPipeline3() {
    return new TestPipeline("PIPELINE_SUCCESS_3_" + ID, StageExecutorState.SUCCESS);
  }

  @Bean
  public TestPipeline errorPipeline() {
    return new TestPipeline("PIPELINE_ERROR_1_" + ID, StageExecutorState.EXECUTION_ERROR);
  }

  @Bean
  public TestSchedule successSchedule() {
    return new TestSchedule(
        "SCHEDULE_SUCCESS_1_" + ID,
        builder ->
            builder
                .execute("SCHEDULE_STAGE_SUCCESS_1")
                .withAsyncTestExecutor(StageExecutorState.SUCCESS, SUBMIT_TIME, EXECUTION_TIME));
  }

  @Bean
  public TestSchedule errorSchedule() {
    return new TestSchedule(
        "SCHEDULE_ERROR_1_" + ID,
        builder ->
            builder
                .execute("SCHEDULE_STAGE_ERROR_1")
                .withAsyncTestExecutor(
                    StageExecutorState.EXECUTION_ERROR, SUBMIT_TIME, EXECUTION_TIME));
  }

  public static class TestPipeline extends ConfigurableTestPipeline<TestProcessConfiguration> {

    public TestPipeline(String pipelineName, StageExecutorState state) {
      super(
          PROCESS_PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration(pipelineName) {
            @Override
            public void configureProcess(ProcessBuilder builder) {
              ExecutorParameters executorParams =
                  ExecutorParameters.builder().immediateRetries(0).maximumRetries(0).build();
              builder
                  .execute("PIPELINE_STAGE_" + state.name() + "_1")
                  .withAsyncTestExecutor(state, SUBMIT_TIME, EXECUTION_TIME, executorParams);
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

  public static void main(String[] args) {

    System.setProperty("pipelite.service.name", "PipeliteWebServerTest_" + ID);
    System.setProperty(
        "pipelite.datasource.driverClassName", System.getenv("PIPELITE_TEST_DATABASE_DRIVER"));
    System.setProperty("pipelite.datasource.url", System.getenv("PIPELITE_TEST_DATABASE_URL"));
    System.setProperty(
        "pipelite.datasource.username", System.getenv("PIPELITE_TEST_DATABASE_USERNAME"));
    System.setProperty(
        "pipelite.datasource.password", System.getenv("PIPELITE_TEST_DATABASE_PASSWORD"));
    Pipelite.main(new String[0]);
  }
}
