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
import java.util.ArrayList;
import java.util.List;

import lombok.extern.flogger.Flogger;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.instance.TaskInstance;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;

@Flogger
public class LSFTaskExecutor extends AbstractTaskExecutor {

  public static final int LSF_JVM_MEMORY_DELTA_MB = 1500;
  public static final int LSF_JVM_MEMORY_OVERHEAD_MB = 200;
  public static final int LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES = 60;

  public LSFTaskExecutor(
      ProcessConfiguration processConfiguration, TaskConfiguration taskConfiguration) {
    super(processConfiguration, taskConfiguration);
  }

  private List<String> constructArgs(TaskInstance instance, boolean commit) {
    List<String> p_args = new ArrayList<>();

    p_args.add("-XX:+UseSerialGC");

    int lsf_memory_limit = instance.getMemory();
    if (lsf_memory_limit <= 0) {
      lsf_memory_limit = taskConfiguration.getMemory();
    }

    int java_memory_limit = lsf_memory_limit - LSF_JVM_MEMORY_DELTA_MB;

    if (0 >= java_memory_limit) {
      log.atWarning().log(
          "LSF memory is lower than "
              + LSF_JVM_MEMORY_DELTA_MB
              + "MB. Java memory limit will not be set.");
    } else {
      p_args.add(String.format("-Xmx%dM", java_memory_limit));
    }

    p_args.addAll(getEnvAsJavaSystemPropertyOptions());

    p_args.add("-cp");
    p_args.add(System.getProperty("java.class.path"));
    p_args.add(StageLauncher.class.getName());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ID);
    p_args.add(instance.getPipeliteStage().getProcessId());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_STAGE);
    p_args.add(instance.getPipeliteStage().getStageName());

    if (commit)
      p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_FORCE_COMMIT);

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ENABLED);
    p_args.add(Boolean.toString(instance.getPipeliteStage().getEnabled()).toLowerCase());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_EXEC_COUNT);
    p_args.add(Long.toString(instance.getPipeliteStage().getExecutionCount()));

    return p_args;
  }

  private LSFBackEnd configureBackend(TaskInstance taskInstance) {

    String queue = taskInstance.getQueue();

    Integer memory = taskInstance.getMemory();
    if (memory == null) {
      memory = LSF_JVM_MEMORY_DELTA_MB + LSF_JVM_MEMORY_OVERHEAD_MB;
      log.atWarning().log("Using default memory: " + memory);
    }

    Integer memoryTimeout = taskInstance.getMemoryTimeout();
    if (memoryTimeout == null) {
      memoryTimeout = LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES;
      log.atWarning().log("Using default memory reservation timeout: " + memoryTimeout);
    }

    Integer cores = taskInstance.getCores();
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

  public ExecutionInfo execute(TaskInstance instance) {

    boolean do_commit = true;
    List<String> p_args = constructArgs(instance, do_commit);

    LSFBackEnd back_end = configureBackend(instance);

    LSFClusterCall call =
        back_end.new_call_instance(
            String.format(
                "%s--%s--%s",
                instance.getPipeliteStage().getProcessName(),
                instance.getPipeliteStage().getProcessId(),
                instance.getPipeliteStage().getStageName()),
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
