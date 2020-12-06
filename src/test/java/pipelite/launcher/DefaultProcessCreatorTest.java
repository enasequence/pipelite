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
package pipelite.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import org.junit.jupiter.api.Test;
import pipelite.TestProcessSource;
import pipelite.UniqueStringGenerator;
import pipelite.entity.ProcessEntity;
import pipelite.service.ProcessService;

public class DefaultProcessCreatorTest {

  @Test
  public void test() {
    final int processCnt = 100;
    String pipelineName = UniqueStringGenerator.randomPipelineName();
    ProcessService service = mock(ProcessService.class);
    DefaultProcessCreator creator =
        spy(
            new DefaultProcessCreator(
                new TestProcessSource(pipelineName, processCnt), service, pipelineName));

    when(service.createExecution(any(), any(), any())).thenReturn(mock(ProcessEntity.class));

    assertThat(creator.createProcesses(processCnt)).isEqualTo(processCnt);
    verify(creator, times(processCnt)).createProcess(any());
  }
}
