/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class SimpleSlurmExecutor extends AbstractSlurmExecutor<SimpleSlurmExecutorParameters>
    implements TimeoutExecutor {

  // Json deserialization requires a no argument constructor.
  public SimpleSlurmExecutor() {}

  private static class JobScriptBuilder {
    private final StringBuilder script = new StringBuilder();

    public JobScriptBuilder() {
      line("#!/bin/bash");
    }

    public JobScriptBuilder shortOption(String option) {
      script.append("#SBATCH -");
      script.append(option);
      script.append("\n");
      return this;
    }

    public JobScriptBuilder shortOption(String option, String value) {
      script.append("#SBATCH -");
      script.append(option);
      script.append(" ");
      script.append(value);
      script.append("\n");
      return this;
    }

    public JobScriptBuilder longOption(String option, String value) {
      script.append("#SBATCH --");
      script.append(option);
      script.append("=\"");
      script.append(value);
      script.append("\"\n");
      return this;
    }

    public JobScriptBuilder line(String line) {
      script.append(line);
      script.append("\n");
      return this;
    }

    public String build() {
      return script.toString();
    }
  }

  @Override
  public final String getSubmitCmd(StageExecutorRequest request) {

    String logDir = logFilePathResolver.resolvedPath().dir(request);
    String logFileName = logFilePathResolver.fileName(request);
    String logFile = logDir + "/" + logFileName;

    String jobName =
        request.getPipelineName()
            + ":"
            + request.getStage().getStageName()
            + ":"
            + request.getProcessId();

    JobScriptBuilder script = new JobScriptBuilder();

    // Options

    // TODO: SLURM to create the output directory once version 23.02 is available
    // to preserve SLURM script execution output instead of sending it to dev/null.

    script.longOption("job-name", jobName);
    script.longOption("output", "/dev/null");
    script.longOption("error", "/dev/null");

    Integer cpu = getExecutorParams().getCpu();
    if (cpu != null && cpu > 0) {
      script.shortOption("n", Integer.toString(cpu));
    }

    Integer memory = getExecutorParams().getMemory();
    String memoryUnits = getExecutorParams().getMemoryUnits();

    if (memory != null && memory > 0) {
      String memStr = memory.toString();
      if (null != memoryUnits) {
        memStr += memoryUnits;
      }
      script.longOption("mem", memStr);
    }

    Duration timeout = getExecutorParams().getTimeout();
    if (timeout == null) {
      timeout = DEFAULT_TIMEOUT;
    }
    if (timeout.toMinutes() == 0) {
      timeout = Duration.ofMinutes(1);
    }
    script.shortOption("t", String.valueOf(timeout.toMinutes()));

    String queue = getExecutorParams().getQueue();
    if (queue != null) {
      script.shortOption("p", queue);
    }

    // Create log directory

    script.line("mkdir -p " + logDir);

    // Run command and direct output to log file

    script.line(getCmd() + " > " + logFile + " 2>&1");

    StringBuilder cmd = getSharedSubmitCmd(request);

    return cmd.toString() + " << EOF\n" + script.build() + "EOF";
  }
}
