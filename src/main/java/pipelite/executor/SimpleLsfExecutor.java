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
import org.apache.commons.lang3.StringUtils;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
public class SimpleLsfExecutor extends AbstractLsfExecutor<SimpleLsfExecutorParameters>
    implements TimeoutExecutor {

  // Json deserialization requires a no argument constructor.
  public SimpleLsfExecutor() {}

  @Override
  public final String getSubmitCmd(StageExecutorRequest request) {

    StringBuilder cmd = getSharedSubmitCmd(request);

    Integer cpu = getExecutorParams().getCpu();
    if (cpu != null && cpu > 0) {
      addCmdArgument(cmd, "-n");
      addCmdArgument(cmd, Integer.toString(cpu));
    }

    Integer memory = getExecutorParams().getMemory();
    String memoryUnits = getExecutorParams().getMemoryUnits();

    Duration memoryTimeout = getExecutorParams().getMemoryTimeout();
    if (memory != null && memory > 0) {
      String memStr = memory.toString();
      if (null != memoryUnits) {
        memStr += memoryUnits;
      }
      addCmdArgument(cmd, "-M");
      addCmdArgument(cmd, memStr);
      addCmdArgument(cmd, "-R");
      addCmdArgument(
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
      addCmdArgument(cmd, "-W");
      addCmdArgument(cmd, String.valueOf(timeout.toMinutes()));
    }

    String jobGroup = getExecutorParams().getJobGroup();
    if (jobGroup != null) {
      addCmdArgument(cmd, "-g");
      addCmdArgument(cmd, jobGroup);
    }

    String queue = getExecutorParams().getQueue();
    if (queue != null) {
      addCmdArgument(cmd, "-q");
      addCmdArgument(cmd, queue);
    }

    String jobName = request.getPipelineName() + ":" + request.getStage().getStageName() + ":" + request.getProcessId();
    addCmdArgument(cmd, "-J");
    addCmdArgument(cmd, jobName);

    return cmd.toString() + " " + getCmd();
  }
}
