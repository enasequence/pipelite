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

import org.junit.jupiter.api.Test;

public class SlurmExecutorPollJobsTest {

  @Test
  public void extractSacctJobid() {
    assertThat(SlurmExecutorPollJobs.extractSacctJobId("9332")).isEqualTo("9332");
    assertThat(SlurmExecutorPollJobs.extractSacctJobId("9332.batch")).isEqualTo("9332");
  }

  @Test
  public void extractSacctStep() {
    assertThat(SlurmExecutorPollJobs.extractSacctStep("9332")).isEqualTo("");
    assertThat(SlurmExecutorPollJobs.extractSacctStep("9332.batch")).isEqualTo("batch");
  }

  @Test
  public void extractSacctExitCode() {
    assertThat(SlurmExecutorPollJobs.extractSacctExitCode("1:2")).isEqualTo("1");
    assertThat(SlurmExecutorPollJobs.extractSacctExitCode("2:3")).isEqualTo("2");
  }
}
