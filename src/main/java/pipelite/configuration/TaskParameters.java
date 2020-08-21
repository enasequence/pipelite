package pipelite.configuration;

public interface TaskParameters {

  Integer getMemory();

  Integer getMemoryTimeout();

  Integer getCores();

  String getQueue();

  Integer getRetries();

  String getTempDir();

  String[] getEnv();
}
