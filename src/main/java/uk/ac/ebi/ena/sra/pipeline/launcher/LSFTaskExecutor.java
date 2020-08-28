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
import pipelite.executor.SystemCallInternalTaskExecutor;
import pipelite.executor.TaskExecutor;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;

@Flogger
public class LSFTaskExecutor implements TaskExecutor {

  public static final int LSF_JVM_MEMORY_DELTA_MB = 1500;
  public static final int LSF_JVM_MEMORY_OVERHEAD_MB = 200;
  public static final int LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES = 60;

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
    if (taskInstance.getTaskParameters().getTempDir() != null) {
      back_end.setOutputFolderPath(Paths.get(taskInstance.getTaskParameters().getTempDir()));
    }
    return back_end;
  }

  @Override
  public TaskExecutionResult execute(TaskInstance taskInstance) {

    List<String> p_args = SystemCallInternalTaskExecutor.getArguments(taskInstance);

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

    call.setTaskLostExitCode(
        taskInstance.getResolver().serializer().serialize(TaskExecutionResult.internalError()));

    log.atInfo().log(call.getCommandLine());

    try {
      call.execute();
      int exitCode = call.getExitCode();
      TaskExecutionResult result = taskInstance.getResolver().serializer().deserialize(exitCode);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, call.getHost());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, call.getCommandLine());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_EXIT_CODE, exitCode);
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDOUT, call.getStdout());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_STDERR, call.getStderr());
      return result;
    } catch (Exception ex) {
      TaskExecutionResult result = TaskExecutionResult.internalError();
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_HOST, call.getHost());
      result.addAttribute(TaskExecutionResult.STANDARD_ATTRIBUTE_COMMAND, call.getCommandLine());
      result.addExceptionAttribute(ex);
      return result;
    }
  }
}
