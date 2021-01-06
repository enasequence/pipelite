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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class AbstractLsfExecutorExtractTest {

  @Test
  public void extractBsubJobIdSubmitted() {
    assertThat(
            AbstractLsfExecutor.extractBsubJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");
    assertThat(AbstractLsfExecutor.extractBsubJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
    assertThat(AbstractLsfExecutor.extractBsubJobIdSubmitted("INVALID")).isNull();
  }

  @Test
  public void extractBjobsJobIdNotFound() {
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("Job <345654> is not found."))
        .isTrue();
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("Job <345654> is not found")).isTrue();
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("Job <345654> is ")).isFalse();
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("INVALID")).isFalse();
  }

  @Test
  public void extractBjobsExitCode() {
    assertThat(AbstractLsfExecutor.extractBjobsExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(AbstractLsfExecutor.extractBjobsExitCode("Exited with exit code 3.")).isEqualTo("3");
    assertThat(AbstractLsfExecutor.extractBjobsExitCode("INVALID")).isNull();
  }

  // TODO:
  public void extractBjobsCustomResult() {}

  // TODO:
  public void extractBjobsStandardResult() {}
}
