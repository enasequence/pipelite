/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.launcher;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;
import pipelite.configuration.*;
import pipelite.service.PipeliteInMemoryLockService;
import pipelite.service.PipeliteInMemoryProcessService;
import pipelite.service.PipeliteInMemoryStageService;

@Slf4j
@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {
      "pipelite.launcher.name=PipeliteInMemoryLauncherTest",
      "pipelite.launcher.workers=5",
      "pipelite.process.name=PipeliteInMemoryLauncherTest",
      "pipelite.process.executor=pipelite.executor.InternalTaskExecutorFactory",
      "pipelite.process.resolver=pipelite.resolver.DefaultExceptionResolver",
      "pipelite.process.stages=uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncherTester$TestStages"
    })
@ActiveProfiles("test")
public class PipeliteInMemoryLauncherTest {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private TaskConfiguration taskConfiguration;
  @Autowired private PipeliteInMemoryProcessService pipeliteProcessService;
  @Autowired private PipeliteInMemoryStageService pipeliteStageService;
  @Autowired private PipeliteInMemoryLockService pipeliteLockService;

  @Test
  public void test() {
    PipeliteLauncherTester tester =
        new PipeliteLauncherTester(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            pipeliteProcessService,
            pipeliteStageService,
            pipeliteLockService);
    tester.test();
  }
}
