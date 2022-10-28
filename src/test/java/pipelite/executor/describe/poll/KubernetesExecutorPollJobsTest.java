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
import static pipelite.executor.describe.poll.KubernetesExecutorPollJobs.extractJobResult;

import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import pipelite.executor.describe.context.request.DefaultRequestContext;

@ActiveProfiles("test")
public class KubernetesExecutorPollJobsTest {

  private static DefaultRequestContext request() {
    return new DefaultRequestContext("validJobId");
  }

  @Test
  public void testExtractJobResultActive() {
    JobStatus jobStatus = new JobStatus();
    assertThat(extractJobResult(request(), jobStatus, () -> null).result.isActive()).isTrue();
    jobStatus.setActive(1);
    assertThat(extractJobResult(request(), jobStatus, () -> null).result.isActive()).isTrue();
  }

  @Test
  public void testExtractJobResultSuccess() {
    JobStatus jobStatus = new JobStatus();
    jobStatus.setCompletionTime("test");
    assertThat(extractJobResult(request(), jobStatus, () -> null).result.isSuccess()).isTrue();

    jobStatus = new JobStatus();
    JobCondition jobCondition = new JobCondition();
    jobCondition.setType("Complete");
    jobCondition.setStatus("true");
    jobStatus.setConditions(List.of(jobCondition));
    assertThat(extractJobResult(request(), jobStatus, () -> null).result.isSuccess()).isTrue();
  }

  @Test
  public void testExtractJobResultError() {
    JobStatus jobStatus = new JobStatus();
    JobCondition jobCondition = new JobCondition();
    jobCondition.setType("Failed");
    jobCondition.setStatus("true");
    jobStatus.setConditions(List.of(jobCondition));
    assertThat(extractJobResult(request(), jobStatus, () -> null).result.isError()).isTrue();
  }
}
