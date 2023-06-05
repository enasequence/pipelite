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
package pipelite.executor.describe;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.configuration.properties.SshTestConfiguration;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.SimpleLsfExecutor;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;
import pipelite.executor.describe.context.executor.LsfExecutorContext;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.test.configuration.PipeliteTestConfigWithServices;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=LsfDescribeJobsTest"})
public class LsfDescribeLostJobsTest {
  @Autowired SshTestConfiguration sshTestConfiguration;

  @Autowired LsfDescribeJobsCache lsfDescribeJobsCache;

  @Autowired PipeliteServices pipeliteServices;

  private static LsfRequestContext requestContext(String jobId) {
    return new LsfRequestContext(jobId, "test");
  }

  @Test
  public void testDescribeLostJobs() {
    List<String> jobIds = Arrays.asList("1", "2", "3", "4", "5");

    SimpleLsfExecutorParameters executorParameters = new SimpleLsfExecutorParameters();
    executorParameters.setHost(sshTestConfiguration.getHost());
    executorParameters.setUser(sshTestConfiguration.getUser());
    executorParameters.setMaximumRetries(1);

    AbstractLsfExecutor<SimpleLsfExecutorParameters> executor = new SimpleLsfExecutor();
    executor.setExecutorParams(executorParameters);

    Stage stage = new Stage("testStageName", executor);
    executor.prepareExecution(pipeliteServices, "testPipelineName", "testProcess", stage);

    DescribeJobs<LsfRequestContext, LsfExecutorContext> describeJobs = executor.getDescribeJobs();

    jobIds.forEach(jobId -> describeJobs.addRequest(requestContext(jobId)));
    assertThat(describeJobs.getActiveRequests().size()).isEqualTo(jobIds.size());

    // DescribeJobs has a worker thread that calls retrieveResults.
    AtomicInteger retrieveResultsCount = new AtomicInteger();
    describeJobs.setRetrieveResultsListener(e -> retrieveResultsCount.incrementAndGet());
    while (retrieveResultsCount.get() == 0) {
      Time.wait(Duration.ofSeconds(1));
    }

    // Check that the results are correct.
    jobIds.forEach(
        jobId -> {
          StageExecutorResult actualResult = describeJobs.getResult(requestContext(jobId));
          assertThat(actualResult.state()).isEqualTo(StageExecutorResult.lostError().state());
        });

    // Check that the requests have been removed.
    jobIds.forEach(jobId -> assertThat(describeJobs.isRequest(requestContext(jobId))).isFalse());
  }
}
