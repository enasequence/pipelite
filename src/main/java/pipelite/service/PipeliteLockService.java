package pipelite.service;

import pipelite.entity.PipeliteProcess;

public interface PipeliteLockService {

    boolean lockLauncher(String launcherName, String processName);

    boolean isLauncherLocked(String launcherName, String processName);

    boolean unlockLauncher(String launcherName, String processName);

    void purgeLauncherLocks(String launcherName, String processName);

    boolean lockProcess(String launcherName, PipeliteProcess pipeliteProcess);

    boolean unlockProcess(String launcherName, PipeliteProcess pipeliteProcess);

    boolean isProcessLocked(PipeliteProcess pipeliteProcess);

}
