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
package pipelite.service;

import lombok.Value;
import pipelite.entity.PipeliteProcess;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeliteInMemoryLockService implements PipeliteLockService {

  @Value
  private static class LauncherLock {
    private final String launcherName;
    private final String processName;
  }

  @Value
  private static class GlobalProcessLock {
    private final String processName;
    private final String processId;
  }

  @Value
  private static class LauncherProcessLock {
    private final String launcherName;
    private final String processName;
    private final String processId;
  }

  private final Set<LauncherLock> launcherLocks = ConcurrentHashMap.newKeySet();
  private final Set<GlobalProcessLock> globalProcessLocks = ConcurrentHashMap.newKeySet();
  private final Set<LauncherProcessLock> launcherProcessLocks = ConcurrentHashMap.newKeySet();

  @Override
  public boolean lockLauncher(String launcherName, String processName) {
    return launcherLocks.add(getLauncherLock(launcherName, processName));
  }

  @Override
  public boolean isLauncherLocked(String launcherName, String processName) {
    return launcherLocks.contains(getLauncherLock(launcherName, processName));
  }

  @Override
  public boolean unlockLauncher(String launcherName, String processName) {
    return launcherLocks.remove(getLauncherLock(launcherName, processName));
  }

  @Override
  public void purgeLauncherLocks(String launcherName, String processName) {
    LauncherLock purgeLauncherLock = new LauncherLock(launcherName, processName);
    launcherLocks.remove(purgeLauncherLock);

    for (LauncherProcessLock launcherProcessLock : launcherProcessLocks) {
      if (launcherProcessLock.getLauncherName().equals(launcherName)
          && launcherProcessLock.getProcessName().equals(processName)) {
        launcherProcessLocks.remove(launcherProcessLock);
        globalProcessLocks.remove(
            new GlobalProcessLock(
                launcherProcessLock.getProcessName(), launcherProcessLock.getProcessId()));
      }
    }
  }

  @Override
  public boolean lockProcess(String launcherName, PipeliteProcess pipeliteProcess) {
    if (globalProcessLocks.add(getGlobalLock(pipeliteProcess))) {
      launcherProcessLocks.add(getLocalLock(launcherName, pipeliteProcess));
      return true;
    }
    return false;
  }

  @Override
  public boolean unlockProcess(String launcherName, PipeliteProcess pipeliteProcess) {
    if (launcherProcessLocks.remove(getLocalLock(launcherName, pipeliteProcess))) {
      globalProcessLocks.remove(getGlobalLock(pipeliteProcess));
      return true;
    }
    return false;
  }

  @Override
  public boolean isProcessLocked(PipeliteProcess pipeliteProcess) {
    return globalProcessLocks.contains(getGlobalLock(pipeliteProcess));
  }

  private static LauncherLock getLauncherLock(String launcherName, String processName) {
    return new LauncherLock(launcherName, processName);
  }

  private static LauncherProcessLock getLocalLock(
      String launcherName, PipeliteProcess pipeliteProcess) {
    return new LauncherProcessLock(
        launcherName, pipeliteProcess.getProcessName(), pipeliteProcess.getProcessId());
  }

  private static GlobalProcessLock getGlobalLock(PipeliteProcess pipeliteProcess) {
    return new GlobalProcessLock(pipeliteProcess.getProcessName(), pipeliteProcess.getProcessId());
  }
}
