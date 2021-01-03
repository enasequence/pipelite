/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.cmd.CmdRunnerResult;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.time.Time;

import java.time.Duration;
import java.time.ZonedDateTime;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class SimpleLsfExecutor extends AbstractLsfExecutor<SimpleLsfExecutorParameters> {

  private static final Duration STDOUT_FILE_POLL_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration STDOUT_FILE_POLL_FREQUENCY = Duration.ofSeconds(10);

  private String stdoutFile;

  @Override
  public final String getPrefixCmd(String pipelineName, String processId, Stage stage) {

    StringBuilder cmd = new StringBuilder();
    cmd.append(BSUB_CMD);

    stdoutFile = getOutFile(pipelineName, processId, stage.getStageName(), "stdout");

    // Write both stderr and stdout to the stdout file.

    addArgument(cmd, "-oo");
    addArgument(cmd, stdoutFile);

    Integer cpu = getExecutorParams().getCpu();
    if (cpu != null && cpu > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cpu));
    }

    Integer memory = getExecutorParams().getMemory();
    Duration memoryTimeout = getExecutorParams().getMemoryTimeout();
    if (memory != null && memory > 0) {
      addArgument(cmd, "-M");
      addArgument(cmd, Integer.toString(memory) + "M"); // Megabytes
      addArgument(cmd, "-R");
      addArgument(
          cmd,
          "\"rusage[mem="
              + memory
              + ((memoryTimeout == null || memoryTimeout.toMinutes() < 0)
                  ? "M" // Megabytes
                  : "M:duration=" + memoryTimeout.toMinutes())
              + "]\"");
    }

    Duration timeout = getExecutorParams().getTimeout();
    if (timeout != null) {
      if (timeout.toMinutes() == 0) {
        timeout = Duration.ofMinutes(1);
      }
      addArgument(cmd, "-W");
      addArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String queue = getExecutorParams().getQueue();
    if (queue != null) {
      addArgument(cmd, "-q");
      addArgument(cmd, queue);
    }

    return cmd.toString();
  }

  @Override
  protected void beforeExecute(String pipelineName, String processId, Stage stage) {}

  @Override
  protected void afterExecute(
      String pipelineName, String processId, Stage stage, StageExecutorResult result) {
    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.

    ZonedDateTime waitUntil = ZonedDateTime.now().plus(STDOUT_FILE_POLL_TIMEOUT);
    while (!stdoutFileExists(cmdRunner, stdoutFile)) {
      Time.waitUntil(STDOUT_FILE_POLL_FREQUENCY, waitUntil);
    }

    logContext(log.atFine(), pipelineName, processId, stage)
        .log("Reading stdout file: %s", stdoutFile);

    try {
      CmdRunnerResult stdoutCmdRunnerResult =
          writeFileToStdout(cmdRunner, stdoutFile, getExecutorParams());
      result.setStdout(stdoutCmdRunnerResult.getStdout());
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read stdout file: %s", stdoutFile);
    }
  }

  private boolean stdoutFileExists(CmdRunner cmdRunner, String stdoutFile) {
    // Check if the stdout file exists. The file may not be immediately available after the job
    // execution finishes.
    return 0
        == cmdRunner
            .execute("sh -c 'test -f " + stdoutFile + "'", getExecutorParams())
            .getExitCode();
  }
}
