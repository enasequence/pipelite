package pipelite.lock;

public interface LauncherInstanceLocker {

  boolean lock(String launcherName, String processName);

  boolean isLocked(String launcherName, String processName);

  boolean unlock(String launcherName, String processName);
}
