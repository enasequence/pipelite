/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.describe.recover;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.configuration.properties.SlurmTestConfiguration;
import pipelite.executor.AbstractSlurmExecutor;
import pipelite.executor.AsyncExecutorTestHelper;
import pipelite.executor.SimpleSlurmExecutor;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.cache.SlurmDescribeJobsCache;
import pipelite.executor.describe.context.executor.SlurmExecutorContext;
import pipelite.executor.describe.context.request.SlurmRequestContext;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithServices;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {
      "pipelite.service.force=true",
      "pipelite.service.name=SlurmExecutorRecoverJobTest"
    })
public class SlurmExecutorRecoverJobTest {

  @Autowired SlurmTestConfiguration slurmTestConfiguration;

  @Autowired SlurmDescribeJobsCache slurmDescribeJobsCache;

  @Autowired PipeliteServices pipeliteServices;

  private SimpleSlurmExecutor executor(int exitCode) {
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    String stageName = PipeliteTestIdCreator.stageName();
    SimpleSlurmExecutor executor = StageExecutor.createSimpleSlurmExecutor("exit " + exitCode);
    executor.setExecutorParams(
        SimpleSlurmExecutorParameters.builder()
            .host(slurmTestConfiguration.getHost())
            .user(slurmTestConfiguration.getUser())
            .logDir(slurmTestConfiguration.getLogDir())
            .queue(slurmTestConfiguration.getQueue())
            .memory(1)
            .cpu(1)
            .timeout(Duration.ofSeconds(30))
            .build());
    Stage stage = new Stage(stageName, executor);
    executor.prepareExecution(pipeliteServices, pipelineName, processId, stage);
    return executor;
  }

  private StageExecutorResult execute(SimpleSlurmExecutor executor) {
    return AsyncExecutorTestHelper.testExecute(executor, pipeliteServices);
  }

  @Test
  public void testRecoverJobCompletedSuccessfully() {
    SimpleSlurmExecutor executor = executor(0);
    StageExecutorResult result = execute(executor);

    String jobId = result.attribute(StageExecutorResultAttribute.JOB_ID);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(jobId).isNotNull();

    SlurmRequestContext requestContext = new SlurmRequestContext(jobId, executor.getOutFile());
    SlurmExecutorContext executorContext =
        slurmDescribeJobsCache.getExecutorContext((AbstractSlurmExecutor) executor);

    DescribeJobsResult<SlurmRequestContext> describeJobsResult =
        (new SlurmExecutorRecoverJob()).recoverJob(executorContext, requestContext);

    assertThat(describeJobsResult.jobId()).isEqualTo(jobId);
    assertThat(describeJobsResult.result.isSuccess()).isTrue();
    assertThat(describeJobsResult.result.attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isEqualTo("0");
  }

  @Test
  public void testRecoverJobExitedWithExitCode() {
    SimpleSlurmExecutor executor = executor(1);
    StageExecutorResult result = execute(executor);

    String jobId = result.attribute(StageExecutorResultAttribute.JOB_ID);

    assertThat(result.isExecutionError()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("1");
    assertThat(jobId).isNotNull();

    SlurmRequestContext requestContext = new SlurmRequestContext(jobId, executor.getOutFile());
    SlurmExecutorContext executorContext =
        slurmDescribeJobsCache.getExecutorContext((AbstractSlurmExecutor) executor);

    DescribeJobsResult<SlurmRequestContext> describeJobsResult =
        (new SlurmExecutorRecoverJob()).recoverJob(executorContext, requestContext);

    assertThat(describeJobsResult.jobId()).isEqualTo(jobId);
    assertThat(describeJobsResult.result.isExecutionError()).isTrue();
    assertThat(describeJobsResult.result.attribute(StageExecutorResultAttribute.EXIT_CODE))
        .isEqualTo("1");
  }

  @Test
  public void testRecoverJobLost() {
    SimpleSlurmExecutor executor = executor(0);
    String jobId = "invalid";

    SlurmRequestContext requestContext = new SlurmRequestContext(jobId, executor.getOutFile());
    SlurmExecutorContext executorContext =
        slurmDescribeJobsCache.getExecutorContext((AbstractSlurmExecutor) executor);

    DescribeJobsResult<SlurmRequestContext> describeJobsResult =
        (new SlurmExecutorRecoverJob()).recoverJob(executorContext, requestContext);

    assertThat(describeJobsResult.jobId()).isEqualTo(jobId);
    assertThat(describeJobsResult.result.isLostError()).isTrue();
  }
}
