package pipelite.service;

public interface PipeliteLockService {

    boolean lockLauncher(String launcherName, String processName);

    boolean isLauncherLocked(String launcherName, String processName);

    boolean unlockLauncher(String launcherName, String processName);

    void purgeLauncherLocks(String launcherName, String processName);

    boolean lockProcess(String launcherName, String processName, String processId);

    boolean unlockProcess(String launcherName, String processName, String processId);

    boolean isProcessLocked(String processName, String processId);

    boolean isProcessLocked(String launcherName, String processName, String processId);
}
