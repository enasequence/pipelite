/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.nio.file.Paths;
import java.util.List;

import lombok.extern.flogger.Flogger;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.instance.TaskInstance;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;

import static uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor.callInternalTaskExecutor;

@Flogger
public class LSFTaskExecutor extends AbstractTaskExecutor {

  public static final int LSF_JVM_MEMORY_DELTA_MB = 1500;
  public static final int LSF_JVM_MEMORY_OVERHEAD_MB = 200;
  public static final int LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES = 60;

  public LSFTaskExecutor(TaskConfigurationEx taskConfiguration) {
    super(taskConfiguration);
  }

  private LSFBackEnd configureBackend(TaskInstance taskInstance) {

    String queue = taskInstance.getTaskParameters().getQueue();

    Integer memory = taskInstance.getTaskParameters().getMemory();
    if (memory == null) {
      memory = LSF_JVM_MEMORY_DELTA_MB + LSF_JVM_MEMORY_OVERHEAD_MB;
      log.atWarning().log("Using default memory: " + memory);
    }

    Integer memoryTimeout = taskInstance.getTaskParameters().getMemoryTimeout();
    if (memoryTimeout == null) {
      memoryTimeout = LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES;
      log.atWarning().log("Using default memory reservation timeout: " + memoryTimeout);
    }

    Integer cores = taskInstance.getTaskParameters().getCores();
    if (cores == null) {
      cores = 1;
      log.atWarning().log("Using default number of cores: " + cores);
    }

    LSFBackEnd back_end = new LSFBackEnd(queue, memory, memoryTimeout, cores);
    if (taskConfiguration.getTempDir() != null) {
      back_end.setOutputFolderPath(Paths.get(taskConfiguration.getTempDir()));
    }
    return back_end;
  }

  public ExecutionInfo execute(TaskInstance taskInstance) {

    List<String> p_args = callInternalTaskExecutor(taskInstance);

    LSFBackEnd back_end = configureBackend(taskInstance);

    LSFClusterCall call =
        back_end.new_call_instance(
            String.format(
                "%s--%s--%s",
                taskInstance.getProcessName(),
                taskInstance.getProcessId(),
                taskInstance.getTaskName()),
            "java",
            p_args.toArray(new String[0]));

    call.setTaskLostExitCode(resolver.exitCodeSerializer().serialize(internalError));

    log.atInfo().log(call.getCommandLine());

    call.execute();

    ExecutionInfo info = new ExecutionInfo();
    info.setCommandline(call.getCommandLine());
    info.setStdout(call.getStdout());
    info.setStderr(call.getStderr());
    info.setExitCode(call.getExitCode());
    info.setHost(call.getHost());
    info.setPID(call.getPID());
    info.setThrowable(null);
    info.setLogMessage(new ExternalCallException(call).toString());
    return info;
  }
}
