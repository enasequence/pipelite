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
package pipelite.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteTestConfiguration;

@SpringBootTest(classes = PipeliteTestConfiguration.class)
@ActiveProfiles(value = {"hsql-test", "pipelite-test"})
@Transactional
@DirtiesContext
class ProcessServiceHsqlTest {

  @Autowired ProcessService service;

  @Test
  public void lifecycle() {
    new ProcessServiceTester(service).lifecycle();
  }

  @Test
  public void testGetActiveCompletedFailedPendingProcessesWithSamePriority() {
    new ProcessServiceTester(service)
        .testGetActiveCompletedFailedPendingProcessesWithSamePriority();
  }

  @Test
  public void testGetActiveCompletedFailedPendingProcessesWithDifferentPriority() {
    new ProcessServiceTester(service)
        .testGetActiveCompletedFailedPendingProcessesWithDifferentPriority();
  }

  @Test
  public void testGetProcesses() {
    new ProcessServiceTester(service).testGetProcesses();
  }

  @Test
  public void testProcessStateSummary() {
    new ProcessServiceTester(service).testProcessStateSummary();
  }
}
