package pipelite.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.entity.PipeliteLock;
import pipelite.entity.PipeliteProcess;
import pipelite.repository.PipeliteLockRepository;

import javax.transaction.Transactional;
import java.util.Optional;

// TODO: pipelite.launcher lock is stored as a process lock and there is a possibility of lock name conflicr

@Service
@Slf4j
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
      log.error("Failed to lock pipelite.launcher {} process {}", launcherName, processName);
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
      log.error("Failed to unlock pipelite.launcher {} process {}", launcherName, processName);
      return false;
    }
  }

  @Override
  @Transactional
  public void purgeLauncherLocks(String launcherName, String processName) {
    repository.deleteByLauncherNameAndProcessName(launcherName, processName);
  }

  @Override
  public boolean lockProcess(String launcherName, PipeliteProcess pipeliteProcess) {
    try {
      return lock(getProcessLock(launcherName, pipeliteProcess));
    } catch (Exception ex) {
      log.error(
          "Failed to lock pipelite.launcher {} process {} instance {}",
          launcherName,
          pipeliteProcess.getProcessName(),
          pipeliteProcess.getProcessId());
      return false;
    }
  }

  @Override
  public boolean unlockProcess(String launcherName, PipeliteProcess pipeliteProcess) {
    try {
      return unlock(getProcessLock(launcherName, pipeliteProcess));
    } catch (Exception ex) {
      log.error(
          "Failed to unlock pipelite.launcher {}  process {} instance {}",
          launcherName,
          pipeliteProcess.getProcessName(),
          pipeliteProcess.getProcessId());
      return false;
    }
  }

  @Override
  public boolean isProcessLocked(PipeliteProcess pipeliteProcess) {
    return isLocked(pipeliteProcess.getProcessName(), pipeliteProcess.getProcessId());
  }

  private static PipeliteLock getLauncherLock(String launcherName, String processName) {
    return new PipeliteLock(launcherName, processName, launcherName);
  }

  private static PipeliteLock getProcessLock(String launcherName, PipeliteProcess pipeliteProcess) {
    return new PipeliteLock(
        launcherName, pipeliteProcess.getProcessName(), pipeliteProcess.getProcessId());
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
        log.error(
            "Failed to unlock pipelite.launcher {} process {} lock {}. Lock owned by different pipelite.launcher {}.",
            pipeliteLock.getLauncherName(),
            pipeliteLock.getProcessName(),
            pipeliteLock.getLockId(),
            activeLock.get().getLauncherName());
        return false;
      }
      repository.delete(pipeliteLock);
      return true;
    }
    return false;
  }
}
