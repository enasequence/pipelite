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

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class SimpleLsfExecutor extends AbstractLsfExecutor<SimpleLsfExecutorParameters> {

  @Override
  public final String getSubmitCmd(StageExecutorRequest request) {

    StringBuilder cmd = new StringBuilder();
    cmd.append(BSUB_CMD);

    // Write both stderr and stdout to the stdout file.

    addArgument(cmd, "-oo");
    addArgument(cmd, getOutFile());

    Integer cpu = getExecutorParams().getCpu();
    if (cpu != null && cpu > 0) {
      addArgument(cmd, "-n");
      addArgument(cmd, Integer.toString(cpu));
    }

    Integer memory = getExecutorParams().getMemory();
    String memoryUnits = getExecutorParams().getMemoryUnits();
    ;
    Duration memoryTimeout = getExecutorParams().getMemoryTimeout();
    if (memory != null && memory > 0) {
      String memStr = memory.toString();
      if (null != memoryUnits) {
        memStr += memoryUnits;
      }
      addArgument(cmd, "-M");
      addArgument(cmd, memStr);
      addArgument(cmd, "-R");
      addArgument(
          cmd,
          "\"rusage[mem="
              + memStr
              + ((memoryTimeout == null || memoryTimeout.toMinutes() < 0)
                  ? ""
                  : ":duration=" + memoryTimeout.toMinutes())
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

    return cmd.toString() + " " + getCmd();
  }
}
