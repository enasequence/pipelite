package pipelite.service;

import lombok.extern.flogger.Flogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteLock;
import pipelite.entity.PipeliteProcess;
import pipelite.log.LogKey;
import pipelite.repository.PipeliteLockRepository;

import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

// TODO: pipelite.launcher lock is stored as a process lock and there is a possibility of lock name
// conflict

@Service
@Profile("database")
@Flogger
public class PipeliteDatabaseLockService implements PipeliteLockService {

  private final PipeliteLockRepository repository;

  public PipeliteDatabaseLockService(@Autowired PipeliteLockRepository repository) {
    this.repository = repository;
  }

  @Override
  public boolean lockLauncher(String launcherName, String processName) {
    try {
      return lock(getLauncherLock(launcherName, processName));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .withCause(ex)
          .log("Failed to lock launcher");
      return false;
    }
  }

  @Override
  public boolean isLauncherLocked(String launcherName, String processName) {
    return isLocked(processName, launcherName);
  }

  @Override
  public boolean unlockLauncher(String launcherName, String processName) {
    try {
      return unlock(getLauncherLock(launcherName, processName));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .withCause(ex)
          .log("Failed to unlock launcher");
      return false;
    }
  }

  @Override
  @Transactional
  public void purgeLauncherLocks(String launcherName, String processName) {
    repository.deleteByLauncherNameAndProcessName(launcherName, processName);
  }

  @Override
  public boolean lockProcess(String launcherName, String processName, String processId) {
    try {
      return lock(getProcessLock(launcherName, processName, processId));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to lock process launcher");
      return false;
    }
  }

  @Override
  public boolean unlockProcess(String launcherName, String processName, String processId) {
    try {
      return unlock(getProcessLock(launcherName, processName, processId));
    } catch (Exception ex) {
      log.atSevere()
          .with(LogKey.LAUNCHER_NAME, launcherName)
          .with(LogKey.PROCESS_NAME, processName)
          .with(LogKey.PROCESS_ID, processId)
          .withCause(ex)
          .log("Failed to unlock process launcher");
      return false;
    }
  }

  @Override
  public boolean isProcessLocked(String processName, String processId) {
    return isLocked(processName, processId);
  }

  private static PipeliteLock getLauncherLock(String launcherName, String processName) {
    return new PipeliteLock(launcherName, processName, launcherName);
  }

  private static PipeliteLock getProcessLock(
      String launcherName, String processName, String processId) {
    return new PipeliteLock(launcherName, processName, processId);
  }

  @Transactional
  private boolean lock(PipeliteLock pipeliteLock) {
    if (isLocked(pipeliteLock.getProcessName(), pipeliteLock.getLockId())) {
      return false;
    }
    repository.save(pipeliteLock);
    return true;
  }

  private boolean isLocked(String processName, String lockId) {
    return repository.findByProcessNameAndLockId(processName, lockId).isPresent();
  }

  @Transactional
  private boolean unlock(PipeliteLock pipeliteLock) {
    Optional<PipeliteLock> activeLock =
        repository.findByProcessNameAndLockId(
            pipeliteLock.getProcessName(), pipeliteLock.getLockId());
    if (activeLock.isPresent()) {
      if (!activeLock.get().getLauncherName().equals(pipeliteLock.getLauncherName())) {
        log.atSevere()
            .with(LogKey.LAUNCHER_NAME, pipeliteLock.getLauncherName())
            .with(LogKey.PROCESS_NAME, pipeliteLock.getProcessName())
            .with(LogKey.PROCESS_ID, pipeliteLock.getLockId())
            .log(
                "Failed to unlock lock. Lock held by different launcher %s",
                activeLock.get().getLauncherName());
        return false;
      }
      repository.delete(pipeliteLock);
      return true;
    }
    return false;
  }
}
