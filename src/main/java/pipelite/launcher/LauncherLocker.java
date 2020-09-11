package pipelite.launcher;

import com.google.common.flogger.FluentLogger;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;
import pipelite.service.PipeliteLockService;

@Value
@Flogger
public class LauncherLocker {

  private final String launcherName;
  private final PipeliteLockService pipeliteLockService;

  public boolean lockLauncher() {
    logContext(log.atInfo()).log("Attempting to lock launcher");

    if (pipeliteLockService.lockLauncher(launcherName, launcherName)) {
      logContext(log.atInfo()).log("Locked launcher");
      return true;
    }
    logContext(log.atWarning()).log("Failed to lock launcher");
    return false;
  }

  public void unlockLauncher() {
    logContext(log.atInfo()).log("Attempting to unlock launcher");

    if (pipeliteLockService.unlockLauncher(launcherName, launcherName)) {
      logContext(log.atInfo()).log("Unlocked launcher");
    } else {
      logContext(log.atInfo()).log("Failed to unlock launcher");
    }
  }

  public boolean lockProcess(String processName, String processId) {
    logContext(log.atInfo(), processName, processId).log("Attempting to lock process");

    if (pipeliteLockService.lockProcess(launcherName, processName, processId)) {
      logContext(log.atInfo(), processName, processId).log("Locked process");
      return true;
    } else {
      if (pipeliteLockService.isProcessLocked(launcherName, processName, processId)) {
        logContext(log.atInfo(), processName, processId).log("Process already locked");
        return true;
      }

      logContext(log.atWarning(), processName, processId).log("Failed to lock process");
      return false;
    }
  }

  public void unlockProcess(String processName, String processId) {
    logContext(log.atInfo(), processName, processId).log("Attempting to unlock process launcher");
    if (pipeliteLockService.unlockProcess(launcherName, processName, processId)) {
      logContext(log.atInfo(), processName, processId).log("Unlocked process launcher");
    } else {
      logContext(log.atInfo(), processName, processId).log("Failed to unlock process launcher");
    }
  }

  private FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName);
  }

  private FluentLogger.Api logContext(FluentLogger.Api log, String processName, String processId) {
    return log.with(LogKey.LAUNCHER_NAME, launcherName)
        .with(LogKey.PROCESS_NAME, processName)
        .with(LogKey.PROCESS_ID, processId);
  }
}
