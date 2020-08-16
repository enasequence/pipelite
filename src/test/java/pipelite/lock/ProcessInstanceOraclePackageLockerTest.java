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
package pipelite.lock;

import org.junit.jupiter.api.Test;
import pipelite.process.instance.ProcessInstance;
import uk.ac.ebi.ena.sra.pipeline.TestConnectionFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessInstanceOraclePackageLockerTest {

  static final private String processName = "TEST";

  @Test
  public void test() {
    ProcessInstanceOraclePackageLocker locker = new ProcessInstanceOraclePackageLocker(TestConnectionFactory.createConnection());

    String launcherName1 = "TEST1";
    String launcherName2 = "TEST2";

    locker.purge(launcherName1, processName);
    locker.purge(launcherName2, processName);

    assertTrue(locker.lock(launcherName1, getProcessInstance("1")));
    assertTrue(locker.isLocked(getProcessInstance("1")));

    assertTrue(locker.lock(launcherName1, getProcessInstance("2")));
    assertTrue(locker.isLocked(getProcessInstance("1")));
    assertTrue(locker.isLocked(getProcessInstance("2")));

    assertTrue(locker.unlock(launcherName1, getProcessInstance("1")));
    assertFalse(locker.isLocked(getProcessInstance("1")));
    assertTrue(locker.isLocked(getProcessInstance("2")));


    assertTrue(locker.lock(launcherName2, getProcessInstance("3")));
    assertFalse(locker.isLocked(getProcessInstance("1")));
    assertTrue(locker.isLocked(getProcessInstance("2")));
    assertTrue(locker.isLocked(getProcessInstance("3")));

    assertTrue(locker.lock(launcherName2, getProcessInstance("4")));
    assertFalse(locker.isLocked(getProcessInstance("1")));
    assertTrue(locker.isLocked(getProcessInstance("2")));
    assertTrue(locker.isLocked(getProcessInstance("3")));
    assertTrue(locker.isLocked(getProcessInstance("4")));

    assertTrue(locker.unlock(launcherName2, getProcessInstance("4")));
    assertFalse(locker.isLocked(getProcessInstance("1")));
    assertTrue(locker.isLocked(getProcessInstance("2")));
    assertTrue(locker.isLocked(getProcessInstance("3")));
    assertFalse(locker.isLocked(getProcessInstance("4")));

    locker.purge(launcherName1, processName);

    assertFalse(locker.isLocked(getProcessInstance("1")));
    assertFalse(locker.isLocked(getProcessInstance("2")));
    assertTrue(locker.isLocked(getProcessInstance("3")));
    assertFalse(locker.isLocked(getProcessInstance("4")));

    locker.purge(launcherName2, processName);

    assertFalse(locker.isLocked(getProcessInstance("1")));
    assertFalse(locker.isLocked(getProcessInstance("2")));
    assertFalse(locker.isLocked(getProcessInstance("3")));
    assertFalse(locker.isLocked(getProcessInstance("4")));
  }

  private static ProcessInstance getProcessInstance(String processId) {
    ProcessInstance processInstance = new ProcessInstance();
    processInstance.setPipelineName(processName);
    processInstance.setProcessId(processId);
    return processInstance;
  }
}
