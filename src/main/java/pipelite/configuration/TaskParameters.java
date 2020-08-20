package pipelite.configuration;

public interface TaskParameters {

  Integer getMemory();

  Integer getMemoryTimeout();

  Integer getCores();

  String getQueue();

  int getRetries();

  String getTempDir();

  String[] getEnv();
}
