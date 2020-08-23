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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import pipelite.TestConfiguration;
import pipelite.configuration.*;
import pipelite.service.PipeliteDatabaseLockService;
import pipelite.service.PipeliteDatabaseProcessService;
import pipelite.service.PipeliteDatabaseStageService;

@Slf4j
@SpringBootTest(
    classes = TestConfiguration.class,
    properties = {
      "pipelite.launcher.name=PipeliteDatabaseLauncherTest",
      "pipelite.launcher.workers=1",
      "pipelite.process.name=PipeliteDatabaseLauncherTest",
      "pipelite.process.executor=pipelite.executor.InternalTaskExecutorFactory",
      "pipelite.process.resolver=pipelite.resolver.DefaultExceptionResolver",
      "pipelite.process.stages=uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncherTester$TestStages"
    })
@ActiveProfiles("test")
public class PipeliteDatabaseLauncherTest {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private TaskConfiguration taskConfiguration;
  @Autowired private PipeliteDatabaseProcessService pipeliteProcessService;
  @Autowired private PipeliteDatabaseStageService pipeliteStageService;
  @Autowired private PipeliteDatabaseLockService pipeliteLockService;

  @Autowired private PlatformTransactionManager transactionManager;

//  @Test
  public void test() {

    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

    transactionTemplate.execute(
        status -> {
          PipeliteLauncherTester tester =
              new PipeliteLauncherTester(
                  launcherConfiguration,
                  processConfiguration,
                  taskConfiguration,
                  pipeliteProcessService,
                  pipeliteStageService,
                  pipeliteLockService);
          tester.test();
          status.setRollbackOnly();
          return true;
        });
  }
}
