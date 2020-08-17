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

import lombok.Value;
import pipelite.entity.PipeliteProcess;

import java.sql.Connection;

@Value
public class LauncherInstanceOraclePackageLocker implements LauncherInstanceLocker {

  private final ProcessInstanceOraclePackageLocker locker;

  public LauncherInstanceOraclePackageLocker(Connection connection) {
    this.locker = new ProcessInstanceOraclePackageLocker(connection);
  }

  @Override
  public boolean lock(String launcherName, String processName) {
    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessName(processName);
    pipeliteProcess.setProcessId(launcherName);
    return locker.lock(launcherName, pipeliteProcess);
  }

  @Override
  public boolean isLocked(String launcherName, String processName) {
    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessName(processName);
    pipeliteProcess.setProcessId(launcherName);
    return locker.isLocked(pipeliteProcess);
  }

  @Override
  public boolean unlock(String launcherName, String processName) {
    PipeliteProcess pipeliteProcess = new PipeliteProcess();
    pipeliteProcess.setProcessName(processName);
    pipeliteProcess.setProcessId(launcherName);
    return locker.unlock(launcherName, pipeliteProcess);
  }
}
