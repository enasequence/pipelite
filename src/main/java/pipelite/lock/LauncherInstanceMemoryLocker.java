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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Value
public class LauncherInstanceMemoryLocker implements LauncherInstanceLocker {

  @Value
  private static class Lock {
    private final String launcherName;
    private final String processName;
  }

  private final Set<Lock> locks = ConcurrentHashMap.newKeySet();

  @Override
  public boolean lock(String launcherName, String processName) {
    return locks.add(getLock(launcherName, processName));
  }

  @Override
  public boolean isLocked(String launcherName, String processName) {
    return locks.contains(getLock(launcherName, processName));
  }

  @Override
  public boolean unlock(String launcherName, String processName) {
    return locks.remove(getLock(launcherName, processName));
  }

  private static Lock getLock(String launcherName, String processName) {
    return new Lock(launcherName, processName);
  }
}
