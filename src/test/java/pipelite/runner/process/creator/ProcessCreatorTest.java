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
import static org.mockito.internal.verification.VerificationModeFactory.times;

import org.junit.jupiter.api.Test;
import pipelite.entity.ProcessEntity;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.tester.pipeline.ConfigurableTestPipeline;
import pipelite.tester.process.TestProcessConfiguration;

public class ProcessCreatorTest {

  private static final int PROCESS_CNT = 100;
  private static final int PARALLELISM = 1;

  private static final class TestPipeline
      extends ConfigurableTestPipeline<TestProcessConfiguration> {
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
    ProcessCreator creator = spy(new ProcessCreator(testPipeline, service));

    when(service.createExecution(any(), any(), any())).thenReturn(mock(ProcessEntity.class));

    assertThat(creator.createProcesses(PROCESS_CNT)).isEqualTo(PROCESS_CNT);
    verify(creator, times(PROCESS_CNT)).createProcess(any());
  }
}
