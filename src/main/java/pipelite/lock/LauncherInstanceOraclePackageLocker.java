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
import org.apache.log4j.Logger;
import pipelite.process.instance.ProcessInstance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Value
public class LauncherInstanceOraclePackageLocker implements LauncherInstanceLocker {

  private final ProcessInstanceOraclePackageLocker locker;

  public LauncherInstanceOraclePackageLocker(Connection connection) {
    this.locker = new ProcessInstanceOraclePackageLocker(connection);
  }

  @Override
  public boolean lock(String launcherId, String processName) {
    ProcessInstance launcherInstance = new ProcessInstance();
    launcherInstance.setPipelineName(processName);
    launcherInstance.setProcessId(launcherId);
    return locker.lock(launcherId, launcherInstance);
  }

  @Override
  public boolean isLocked(String launcherId, String processName) {
    ProcessInstance launcherInstance = new ProcessInstance();
    launcherInstance.setPipelineName(processName);
    launcherInstance.setProcessId(launcherId);
    return locker.isLocked(launcherInstance);
  }

  @Override
  public boolean unlock(String launcherId, String processName) {
    ProcessInstance launcherInstance = new ProcessInstance();
    launcherInstance.setPipelineName(processName);
    launcherInstance.setProcessId(launcherId);
    return locker.unlock(launcherId, launcherInstance);
  }
}
