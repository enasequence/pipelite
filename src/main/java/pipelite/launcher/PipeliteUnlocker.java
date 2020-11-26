package pipelite.launcher;

import com.google.common.util.concurrent.AbstractScheduledService;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.LauncherLockEntity;
import pipelite.service.LockService;

import java.time.Duration;

@Component
@Flogger
/** Removes expired locks. */
public class PipeliteUnlocker extends AbstractScheduledService {

  private final LauncherConfiguration launcherConfiguration;
  private final LockService lockService;
  private final Duration unlockFrequency;

  public PipeliteUnlocker(
      @Autowired LauncherConfiguration launcherConfiguration, @Autowired LockService lockService) {
    this.launcherConfiguration = launcherConfiguration;
    this.lockService = lockService;

    if (launcherConfiguration.getPipelineUnlockFrequency() != null) {
      this.unlockFrequency = launcherConfiguration.getPipelineUnlockFrequency();
    } else {
      this.unlockFrequency = LauncherConfiguration.DEFAULT_PIPELINE_UNLOCK_FREQUENCY;
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(Duration.ZERO, unlockFrequency);
  }

  @Override
  protected void runOneIteration() {
    if (!isRunning()) {
      return;
    }
    removeExpiredLocks();
  }

  public void removeExpiredLocks() {
    log.atInfo().log("Removing expired locks");
    for (LauncherLockEntity launcherLock : lockService.getExpiredLauncherLocks()) {
      log.atInfo().log("Removing expired locks for %s", launcherLock.getLauncherName());
      lockService.unlockProcesses(launcherLock);
      lockService.unlockLauncher(launcherLock);
    }
  }
}
