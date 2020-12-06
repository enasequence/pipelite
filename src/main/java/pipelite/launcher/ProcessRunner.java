package pipelite.launcher;

import pipelite.process.Process;

import java.time.LocalDateTime;

public interface ProcessRunner {
  /**
   * Runs a process.
   *
   * @param pipelineName the pipeline name
   * @param process the process
   * @param callbacks the process execution callbacks
   */
  void runProcess(String pipelineName, Process process, ProcessRunnerCallback callbacks);

  String getPipelineName();

  String getProcessId();

  LocalDateTime getStartTime();
}
