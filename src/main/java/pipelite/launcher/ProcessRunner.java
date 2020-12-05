package pipelite.launcher;

import pipelite.process.Process;

import java.util.Arrays;
import java.util.Collection;

public interface ProcessRunner {
  /**
   * Runs a process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callback the process execution callback
   */
  default void runProcess(String pipelineName, Process process, ProcessRunnerCallback callback) {
    runProcess(pipelineName, process, Arrays.asList(callback));
  }

  /**
   * Runs a process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callbacks the process execution callbacks
   */
  void runProcess(
      String pipelineName, Process process, Collection<ProcessRunnerCallback> callbacks);

  /**
   * Returns true if the pipeline is active.
   *
   * @param pipelineName the pipeline name
   * @return true if the pipeline is active.
   */
  boolean isPipelineActive(String pipelineName);

  /**
   * Returns true if the process is active.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @return true if the process is active.
   */
  boolean isProcessActive(String pipelineName, String processId);
}
