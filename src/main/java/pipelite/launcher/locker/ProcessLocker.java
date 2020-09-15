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
public class ProcessLocker {

  private final String launcherName;
  private final LockService lockService;

  public boolean lock(String pipelineName, String processId) {
    logContext(log.atInfo(), pipelineName, processId).log("Attempting to lock process");

    if (lockService.lockProcess(launcherName, pipelineName, processId)) {
      logContext(log.atInfo(), pipelineName, processId).log("Locked process");
      return true;
    } else {
      if (lockService.isProcessLocked(launcherName, pipelineName, processId)) {
        logContext(log.atInfo(), pipelineName, processId).log("Process already locked");
        return true;
      }

      logContext(log.atWarning(), pipelineName, processId).log("Failed to lock process");
      return false;
    }
  }

  public void unlock(String pipelineName, String processId) {
    logContext(log.atInfo(), pipelineName, processId).log("Attempting to unlock process launcher");
    if (lockService.unlockProcess(launcherName, pipelineName, processId)) {
      logContext(log.atInfo(), pipelineName, processId).log("Unlocked process launcher");
    } else {
      logContext(log.atInfo(), pipelineName, processId).log("Failed to unlock process launcher");
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String pipelineName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
