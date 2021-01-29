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
package pipelite.launcher.process.creator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import org.junit.jupiter.api.Test;
import pipelite.PrioritizedPipeline;
import pipelite.PrioritizedPipelineTestHelper;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;

public class DefaultPrioritizedProcessCreatorTest {

  private static final int PROCESS_CNT = 100;

  private static final class TestPipeline implements PrioritizedPipeline {
    private final String pipelineName =
        UniqueStringGenerator.randomPipelineName(DefaultPrioritizedProcessCreatorTest.class);
    private final PrioritizedPipelineTestHelper helper =
        new PrioritizedPipelineTestHelper(PROCESS_CNT);

    @Override
    public String pipelineName() {
      return pipelineName;
    }

    @Override
    public Options configurePipeline() {
      return new Options().pipelineParallelism(1);
    }

    @Override
    public PrioritizedProcess nextProcess() {
      return helper.nextProcess();
    }

    @Override
    public void confirmProcess(String processId) {
      helper.confirmProcess(processId);
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {}
  }

  @Test
  public void test() {
    TestPipeline testPipeline = new TestPipeline();

    ProcessService service = mock(ProcessService.class);
    DefaultPrioritizedProcessCreator creator =
        spy(new DefaultPrioritizedProcessCreator(testPipeline, service));

    when(service.createExecution(any(), any(), any())).thenReturn(mock(ProcessEntity.class));

    assertThat(creator.createProcesses(PROCESS_CNT)).isEqualTo(PROCESS_CNT);
    verify(creator, times(PROCESS_CNT)).createProcess(any());
  }
}
