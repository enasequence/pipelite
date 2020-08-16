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

import org.junit.Assert;
import org.junit.Test;
import pipelite.process.instance.ProcessInstance;
import uk.ac.ebi.ena.sra.pipeline.TestConnectionFactory;

public class ProcessInstanceOraclePackageLockerTest {

  static final private String processName = "TEST";

  @Test
  public void test() {
    ProcessInstanceOraclePackageLocker locker = new ProcessInstanceOraclePackageLocker(TestConnectionFactory.createConnection());

    String launcherId1 = "TEST1";
    String launcherId2 = "TEST2";

    locker.purge(launcherId1, processName);
    locker.purge(launcherId2, processName);

    Assert.assertTrue(locker.lock(launcherId1, getProcessInstance("1")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("1")));

    Assert.assertTrue(locker.lock(launcherId1, getProcessInstance("2")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("1")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("2")));

    Assert.assertTrue(locker.unlock(launcherId1, getProcessInstance("1")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("1")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("2")));


    Assert.assertTrue(locker.lock(launcherId2, getProcessInstance("3")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("1")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("2")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("3")));

    Assert.assertTrue(locker.lock(launcherId2, getProcessInstance("4")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("1")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("2")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("3")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("4")));

    Assert.assertTrue(locker.unlock(launcherId2, getProcessInstance("4")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("1")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("2")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("3")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("4")));

    locker.purge(launcherId1, processName);

    Assert.assertFalse(locker.isLocked(getProcessInstance("1")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("2")));
    Assert.assertTrue(locker.isLocked(getProcessInstance("3")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("4")));

    locker.purge(launcherId2, processName);

    Assert.assertFalse(locker.isLocked(getProcessInstance("1")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("2")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("3")));
    Assert.assertFalse(locker.isLocked(getProcessInstance("4")));
  }

  private static ProcessInstance getProcessInstance(String processId) {
    ProcessInstance processInstance = new ProcessInstance();
    processInstance.setPipelineName(processName);
    processInstance.setProcessId(processId);
    return processInstance;
  }
}
