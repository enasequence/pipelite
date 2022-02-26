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
package pipelite.runner.process.creator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

public class ProcessEntityCreatorTest {

  private static final int PROCESS_CNT = 100;
  private static final int PARALLELISM = 1;

  private static class TestPipeline extends ConfigurableTestPipeline<TestProcessConfiguration> {
    public TestPipeline() {
      super(
          PARALLELISM,
          PROCESS_CNT,
          new TestProcessConfiguration() {
            @Override
            protected void configure(ProcessBuilder builder) {}
          });
    }
  }

  @Test
  public void test() {
    TestPipeline testPipeline = new TestPipeline();

    ProcessService service = mock(ProcessService.class);
    ProcessEntityCreator creator = spy(new ProcessEntityCreator(testPipeline, service));

    when(service.createExecution(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              String pipelineName = invocation.getArgument(0);
              String processId = invocation.getArgument(1);
              return processEntity(pipelineName, processId);
            });

    assertThat(creator.create(PROCESS_CNT)).isEqualTo(PROCESS_CNT);
    assertThat(testPipeline.confirmedProcessCount()).isEqualTo(PROCESS_CNT);
  }

  private static ProcessEntity processEntity(String pipelineName, String processId) {
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName(pipelineName);
    processEntity.setProcessId(processId);
    return processEntity;
  }
}
