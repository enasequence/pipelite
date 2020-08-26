package pipelite.instance;

public interface TaskParameters {

  Integer getMemory();

  Integer getMemoryTimeout();

  Integer getCores();

  String getQueue();

  Integer getRetries();

  String getTempDir();

  String[] getEnv();

  void setMemory(Integer memory);

  void setMemoryTimeout(Integer memoryTimeout);

  void setCores(Integer cores);

  void setQueue(String queue);

  void setRetries(Integer retries);

  void setTempDir(String tempDir);

  void setEnv(String[] env);

  /* Add parameters. Existing values will not be replaced. */
  default void add(TaskParameters taskParameters) {
    if (taskParameters == null) {
      return;
    }
    setMemory(TaskParametersUtils.getMemory(this, taskParameters));
    setMemoryTimeout(TaskParametersUtils.getMemoryTimeout(this, taskParameters));
    setCores(TaskParametersUtils.getCores(this, taskParameters));
    setQueue(TaskParametersUtils.getQueue(this, taskParameters));
    setRetries(TaskParametersUtils.getRetries(this, taskParameters));
    setTempDir(TaskParametersUtils.getTempDir(this, taskParameters));
    setEnv(TaskParametersUtils.getEnv(this, taskParameters));
  }
}
