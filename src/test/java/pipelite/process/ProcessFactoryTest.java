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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import pipelite.Pipeline;
import pipelite.entity.ProcessEntity;
import pipelite.exception.PipeliteException;
import pipelite.process.builder.ProcessBuilder;

public class ProcessFactoryTest {

  private static final String PIPELINE_NAME = "PIPELINE1";
  private static final String PROCESS_ID = "PROCESS1";

  @Test
  public void createSuccess() {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PROCESS_ID);
    processEntity.setPipelineName(PIPELINE_NAME);
    Pipeline pipeline =
        new Pipeline() {
          @Override
          public String pipelineName() {
            return PIPELINE_NAME;
          }

          @Override
          public Options configurePipeline() {
            return new Options().pipelineParallelism(5);
          }

          @Override
          public void configureProcess(ProcessBuilder builder) {
            builder.execute("STAGE1").withCallExecutor();
          }
        };

    Process process = ProcessFactory.create(processEntity, pipeline);
    assertThat(process).isNotNull();
    assertThat(process.getProcessId()).isEqualTo(PROCESS_ID);
    assertThat(process.getProcessEntity()).isNotNull();
    assertThat(process.getProcessEntity()).isSameAs(processEntity);
  }

  @Test
  public void createFailed() {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setProcessId(PROCESS_ID);
    Pipeline pipeline =
        new Pipeline() {
          @Override
          public String pipelineName() {
            return PIPELINE_NAME;
          }

          @Override
          public Options configurePipeline() {
            return new Options().pipelineParallelism(5);
          }

          @Override
          public void configureProcess(ProcessBuilder builder) {}
        };

    assertThrows(PipeliteException.class, () -> ProcessFactory.create(processEntity, pipeline));
  }
}
