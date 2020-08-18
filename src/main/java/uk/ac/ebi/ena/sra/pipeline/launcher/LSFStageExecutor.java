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

import pipelite.configuration.LSFTaskExecutorConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskExecutorConfiguration;
import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.instance.TaskInstance;
import pipelite.resolver.ExceptionResolver;
import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;
import pipelite.task.result.TaskExecutionResult;

public class LSFStageExecutor extends AbstractTaskExecutor {
  public static final int LSF_JVM_MEMORY_DELTA_MB = 1500;
  public static final int LSF_JVM_MEMORY_OVERHEAD_MB = 200;
  public static final int LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES = 60;

  ExecutionInfo info;
  private final TaskExecutionResult internalError;
  private final ProcessConfiguration processConfiguration;
  private final TaskExecutorConfiguration taskExecutorConfiguration;
  private final LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration;

  public LSFStageExecutor(
      String pipeline_name,
      ExceptionResolver resolver,
      ProcessConfiguration processConfiguration,
      TaskExecutorConfiguration taskExecutorConfiguration,
      LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration) {
    super(pipeline_name, resolver);
    this.internalError = resolver.internalError();
    this.processConfiguration = processConfiguration;
    this.taskExecutorConfiguration = taskExecutorConfiguration;
    this.lsfTaskExecutorConfiguration = lsfTaskExecutorConfiguration;
  }

  public void reset(TaskInstance instance) {
    instance.getPipeliteStage().resetExecution();
  }

  public String[] getJavaSystemProperties() {
    return processConfiguration.getJavaProperties();
  }

  private List<String> constructArgs(TaskInstance instance, boolean commit) {
    List<String> p_args = new ArrayList<>();

    p_args.add("-XX:+UseSerialGC");

    int lsf_memory_limit = instance.getMemory();
    if (lsf_memory_limit <= 0) {
      lsf_memory_limit = taskExecutorConfiguration.getMemory();
    }

    int java_memory_limit = lsf_memory_limit - LSF_JVM_MEMORY_DELTA_MB;

    if (0 >= java_memory_limit) {
      log.warn(
          "LSF memory is lower than "
              + LSF_JVM_MEMORY_DELTA_MB
              + "MB. Java memory limit will not be set.");
    } else {
      p_args.add(String.format("-Xmx%dM", java_memory_limit));
    }

    String[] prop_pass =
        mergeJavaSystemProperties(getJavaSystemProperties(), instance.getJavaSystemProperties());
    addJavaSystemProperties(p_args, prop_pass);

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

  private LSFBackEnd configureBackend(TaskInstance instance) {
    int mem = instance.getMemory();
    if (mem <= 0) {
      mem = taskExecutorConfiguration.getMemory();
      if (mem <= 0) {
        log.warn(
            "Setting LSF memory limit to default value "
                + LSF_JVM_MEMORY_DELTA_MB
                + LSF_JVM_MEMORY_OVERHEAD_MB
                + "MB.");
        mem = LSF_JVM_MEMORY_DELTA_MB + LSF_JVM_MEMORY_OVERHEAD_MB;
      }
    }
    int cpu = instance.getCores();
    if (cpu <= 0) {
      cpu = taskExecutorConfiguration.getCores();
      if (cpu <= 0) {
        log.warn("Setting CPU cores count to default value 1");
        cpu = 1;
      }
    }
    Integer memoryReservationTimeout = lsfTaskExecutorConfiguration.getMemoryTimeout();
    if (memoryReservationTimeout == null) {
      memoryReservationTimeout = LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES;
      log.warn(
          "Setting LSF memory timeout to default value "
              + LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES
              + "min.");
    }

    LSFBackEnd back_end =
        new LSFBackEnd(lsfTaskExecutorConfiguration.getQueue(), mem, memoryReservationTimeout, cpu);
    if (taskExecutorConfiguration.getTempDir() != null) {
      back_end.setOutputFolderPath(Paths.get(taskExecutorConfiguration.getTempDir()));
    }
    return back_end;
  }

  public void execute(TaskInstance instance) {
    if (TaskExecutionState.ACTIVE == getTaskExecutionState(instance)) {
      log.info(
          String.format(
              "%sxecuting stage %s",
              instance.getPipeliteStage().getExecutionCount() > 0 ? "E" : "Re-e",
              instance.getPipeliteStage().getStageName()));

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
              p_args.toArray(new String[p_args.size()]));

      call.setTaskLostExitCode(resolver.exitCodeSerializer().serialize(internalError));

      log.info(call.getCommandLine());

      call.execute();
      fillExecutionInfo(call);

      // Critical section to avoid constructing large strings multiple times
      synchronized (LSFStageExecutor.class) {
        String print_msg =
            String.format(
                "Finished execution of stage %s\n%s",
                instance.getPipeliteStage().getStageName(),
                new ExternalCallException(call).toString());

        log.info(print_msg);
      }
    }
  }

  private void fillExecutionInfo(ExternalCall ec) {
    info = new ExecutionInfo();
    info.setCommandline(ec.getCommandLine());
    info.setStdout(ec.getStdout());
    info.setStderr(ec.getStderr());
    info.setExitCode(ec.getExitCode());
    info.setHost(ec.getHost());
    info.setPID(ec.getPID());
    info.setThrowable(null);
    info.setLogMessage(new ExternalCallException(ec).toString());
  }

  public ExecutionInfo get_info() {
    return info;
  }
}
