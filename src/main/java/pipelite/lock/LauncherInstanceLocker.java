package pipelite.lock;

public interface LauncherInstanceLocker {

  boolean lock(String launcherId, String processName);

  boolean isLocked(String launcherId, String processName);

  boolean unlock(String launcherId, String processName);
}
