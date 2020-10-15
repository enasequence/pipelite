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
package pipelite.launcher.locker;

import com.google.common.flogger.FluentLogger;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;
import pipelite.service.LockService;

@Value
@Flogger
public class LauncherLocker {

  private final String launcherName;
  private final LockService lockService;

  public boolean lock() {
    logContext(log.atInfo()).log("Attempting to lock launcher");

    if (lockService.lockLauncher(launcherName, launcherName)) {
      logContext(log.atInfo()).log("Locked launcher");
      return true;
    }
    logContext(log.atWarning()).log("Failed to lock launcher");
    return false;
  }

  public void unlock() {
    logContext(log.atInfo()).log("Attempting to unlock launcher");

    if (lockService.unlockLauncher(launcherName, launcherName)) {
      logContext(log.atInfo()).log("Unlocked launcher");
    } else {
      logContext(log.atInfo()).log("Failed to unlock launcher");
    }
  }

  public void removeLocks() {
    logContext(log.atInfo()).log("Attempting to remove launcher locks");

    if (lockService.removeLauncherLocks(launcherName)) {
      logContext(log.atInfo()).log("Removed launcher locks");
    } else {
      logContext(log.atInfo()).log("Failed to remove launcher locks");
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName);
  }
}
