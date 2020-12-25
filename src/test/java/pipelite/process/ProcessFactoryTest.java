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
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.stage.executor.StageExecutorResultType;

public class ProcessFactoryTest {

  private static String PIPELINE_NAME = "PIPELINE1";
  private static String PROCESS_ID = "PROCESS1";

  @Test
  public void createSuccess() {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PROCESS_ID);
    ProcessFactory processFactory =
        new ProcessFactory() {
          @Override
          public String getPipelineName() {
            return PIPELINE_NAME;
          }

          @Override
          public int getProcessParallelism() {
            return 5;
          }

          @Override
          public Process create(String processId) {
            return new ProcessBuilder(processId)
                .execute("STAGE1")
                .withEmptySyncExecutor(StageExecutorResultType.SUCCESS)
                .build();
          }
        };
    Process process = ProcessFactory.create(processEntity, processFactory);
    assertThat(process).isNotNull();
    assertThat(process.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getProcessEntity()).isNotNull();
    assertThat(process.getProcessEntity()).isSameAs(processEntity);
  }

  @Test
  public void createFailed() {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PROCESS_ID);
    ProcessFactory processFactory =
        new ProcessFactory() {
          @Override
          public String getPipelineName() {
            return PIPELINE_NAME;
          }

          @Override
          public int getProcessParallelism() {
            return 5;
          }

          @Override
          public Process create(String processId) {
            return null;
          }
        };
    Process process = ProcessFactory.create(processEntity, processFactory);
    assertThat(process).isNull();
  }
}
