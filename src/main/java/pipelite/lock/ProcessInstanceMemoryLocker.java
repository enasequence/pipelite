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
import pipelite.process.instance.ProcessInstance;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessInstanceMemoryLocker implements ProcessInstanceLocker {

  @Value
  private static class GlobalLock {
    private final String processName;
    private final String processId;
  }

  @Value
  private static class LocalLock {
    private final String launcherName;
    private final String processName;
    private final String processId;
  }

  private final Set<GlobalLock> globalLocks = ConcurrentHashMap.newKeySet();
  private final Set<LocalLock> localLocks = ConcurrentHashMap.newKeySet();

  @Override
  public boolean lock(String launcherName, ProcessInstance processInstance) {
    if (globalLocks.add(getGlobalLock(processInstance))) {
      localLocks.add(getLocalLock(launcherName, processInstance));
      return true;
    }
    return false;
  }

  @Override
  public boolean unlock(String launcherName, ProcessInstance processInstance) {
    if (localLocks.remove(getLocalLock(launcherName, processInstance))) {
      globalLocks.remove(getGlobalLock(processInstance));
      return true;
    }
    return false;
  }

  @Override
  public boolean isLocked(ProcessInstance processInstance) {
    return globalLocks.contains(getGlobalLock(processInstance));
  }

  @Override
  public void purge(String launcherName, String processName) {
    for (LocalLock localLock : localLocks) {
      if (localLock.getLauncherName().equals(launcherName)
              && localLock.getProcessName().equals(processName)) {
        GlobalLock globalLock =
                new GlobalLock(localLock.getProcessName(), localLock.getProcessId());
        localLocks.remove(localLock);
        globalLocks.remove(globalLock);
      }
    }
  }

  private static LocalLock getLocalLock(String launcherName, ProcessInstance processInstance) {
    return new LocalLock(
            launcherName, processInstance.getPipelineName(), processInstance.getProcessId());
  }

  private static GlobalLock getGlobalLock(ProcessInstance processInstance) {
    return new GlobalLock(processInstance.getPipelineName(), processInstance.getProcessId());
  }
}
