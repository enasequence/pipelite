package pipelite.task;

import java.time.Duration;

/**
 * Task parameters that can be configured using TaskConfiguration. If the TaskInstance does not have
 * a TaskParameter value then a value from TaskConfiguration will be used.
 */
public interface ConfigurableTaskParameters {

  String getHost();

  Duration getTimeout();

  Integer getRetries();

  String[] getEnv();

  String getWorkDir();

  Integer getMemory();

  Duration getMemoryTimeout();

  Integer getCores();

  String getQueue();

  Duration getPollDelay();
}
