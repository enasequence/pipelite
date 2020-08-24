package pipelite.launcher;

public interface PipeliteLauncher {

  boolean init();

  void execute();

  void stop();

  void setStopIfEmpty();
}
