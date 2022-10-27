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
package pipelite.executor.describe.poll;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.executor.describe.poll.KubernetesExecutorPollJobs.extractJobResultFromStatus;

import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
public class KubernetesExecutorPollJobsTest {

  @Test
  public void testDescribeJobsStateActive() {
    JobStatus jobStatus = new JobStatus();
    assertThat(extractJobResultFromStatus(jobStatus).isActive()).isTrue();
    jobStatus.setActive(1);
    assertThat(extractJobResultFromStatus(jobStatus).isActive()).isTrue();
  }

  @Test
  public void testDescribeJobsStateSuccess() {
    JobStatus jobStatus = new JobStatus();
    jobStatus.setCompletionTime("test");
    assertThat(extractJobResultFromStatus(jobStatus).isSuccess()).isTrue();

    jobStatus = new JobStatus();
    JobCondition jobCondition = new JobCondition();
    jobCondition.setType("Complete");
    jobCondition.setStatus("true");
    jobStatus.setConditions(Arrays.asList(jobCondition));
    assertThat(extractJobResultFromStatus(jobStatus).isSuccess()).isTrue();
  }

  @Test
  public void testDescribeJobsStateError() {
    JobStatus jobStatus = new JobStatus();
    JobCondition jobCondition = new JobCondition();
    jobCondition.setType("Failed");
    jobCondition.setStatus("true");
    jobStatus.setConditions(Arrays.asList(jobCondition));
    assertThat(extractJobResultFromStatus(jobStatus).isError()).isTrue();
  }
}
