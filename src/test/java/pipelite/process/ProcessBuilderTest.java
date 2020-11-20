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
package pipelite.process;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import pipelite.UniqueStringGenerator;
import pipelite.executor.SuccessSyncExecutor;
import pipelite.process.builder.ProcessBuilder;

public class ProcessBuilderTest {

  private static final String PROCESS_ID = UniqueStringGenerator.randomProcessId();

  @Test
  public void test() {
    String stageName1 = UniqueStringGenerator.randomStageName();
    String stageName2 = UniqueStringGenerator.randomStageName();
    String stageName3 = UniqueStringGenerator.randomStageName();
    String stageName4 = UniqueStringGenerator.randomStageName();

    Process process =
        new ProcessBuilder(PROCESS_ID)
            .execute(stageName1)
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious(stageName2)
            .with(new SuccessSyncExecutor())
            .executeAfterPrevious(stageName3)
            .with(new SuccessSyncExecutor())
            .executeAfterFirst(stageName4)
            .with(new SuccessSyncExecutor())
            .build();

    assertThat(process).isNotNull();
    assertThat(process.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getStages().get(0).getDependsOn()).isNull();
    assertThat(process.getStages().get(1).getDependsOn().getStageName()).isEqualTo(stageName1);
    assertThat(process.getStages().get(2).getDependsOn().getStageName()).isEqualTo(stageName2);
    assertThat(process.getStages().get(3).getDependsOn().getStageName()).isEqualTo(stageName1);
    assertThat(process.getStages()).hasSize(4);
  }
}
