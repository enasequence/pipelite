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
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;

public class AbstractLsfExecutorTest {

  @Test
  public void extractJobIdFromSubmitOutput() {
    assertThat(
            AbstractLsfExecutor.extractJobIdFromSubmitOutput(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");
    assertThat(AbstractLsfExecutor.extractJobIdFromSubmitOutput("Job <2848143> is submitted "))
        .isEqualTo("2848143");
    assertThrows(
        PipeliteException.class, () -> AbstractLsfExecutor.extractJobIdFromSubmitOutput("INVALID"));
  }

  @Test
  public void getTerminateCmd() {
    assertThat(AbstractLsfExecutor.getTerminateCmd("test")).isEqualTo("bkill test");
  }
}
