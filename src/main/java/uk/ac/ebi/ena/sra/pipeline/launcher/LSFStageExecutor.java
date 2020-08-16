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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import pipelite.configuration.LSFTaskExecutorConfiguration;
import pipelite.configuration.TaskExecutorConfiguration;
import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.instance.LatestTaskExecution;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import pipelite.task.result.TaskExecutionResult;

public class LSFStageExecutor extends AbstractTaskExecutor {
  public static final int LSF_JVM_MEMORY_DELTA_MB = 1500;
  public static final int LSF_JVM_MEMORY_OVERHEAD_MB = 200;
  public static final int LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES = 60;

  ExecutionInfo info;
  private final String config_prefix_name;
  private final String config_source_name;
  private final TaskExecutionResult internalError;
  private final String[] properties_pass;
  private final TaskExecutorConfiguration taskExecutorConfiguration;
  private final LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration;

  public LSFStageExecutor(
      String pipeline_name,
      TaskExecutionResultExceptionResolver resolver,
      TaskExecutorConfiguration taskExecutorConfiguration,
      LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration) {
    this(
        pipeline_name,
        resolver,
        DefaultConfiguration.CURRENT.getConfigPrefixName(),
        DefaultConfiguration.CURRENT.getConfigSourceName(),
        DefaultConfiguration.CURRENT.getPropertiesPass(),
        taskExecutorConfiguration,
        lsfTaskExecutorConfiguration);
  }

  LSFStageExecutor(
      String pipeline_name,
      TaskExecutionResultExceptionResolver resolver,
      String config_prefix_name,
      String config_source_name,
      String[] properties_pass,
      TaskExecutorConfiguration taskExecutorConfiguration,
      LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration) {
    super(pipeline_name, resolver);
    this.internalError = resolver.internalError();
    this.config_prefix_name = config_prefix_name;
    this.config_source_name = config_source_name;
    this.properties_pass = properties_pass;

    this.taskExecutorConfiguration = taskExecutorConfiguration;
    this.lsfTaskExecutorConfiguration = lsfTaskExecutorConfiguration;
  }

  public void reset(TaskInstance instance) {
    instance.setLatestTaskExecution(new LatestTaskExecution());
  }

  public String[] getPropertiesPass() {
    return properties_pass;
  }

  private String[] mergePropertiesPass(String[] pp1, String[] pp2) {
    if (pp1 == null) {
      pp1 = new String[0];
    }
    if (pp2 == null) {
      pp2 = new String[0];
    }
    Set<String> set1 = new HashSet<>(Arrays.asList(pp1));
    Set<String> set2 = new HashSet<>(Arrays.asList(pp2));
    set1.addAll(set2);
    return set1.toArray(new String[set1.size()]);
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

    p_args.add(String.format("-D%s=%s", config_prefix_name, config_source_name));

    String[] prop_pass = mergePropertiesPass(getPropertiesPass(), instance.getPropertiesPass());
    appendProperties(p_args, prop_pass);

    p_args.add("-cp");
    p_args.add(System.getProperty("java.class.path"));
    p_args.add(StageLauncher.class.getName());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ID);
    p_args.add(instance.getProcessId());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_STAGE);
    p_args.add(instance.getTaskName());

    if (commit)
      p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_FORCE_COMMIT);

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ENABLED);
    p_args.add(Boolean.toString(instance.isEnabled()).toLowerCase());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_EXEC_COUNT);
    p_args.add(Long.toString(instance.getExecutionCount()));

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
              instance.getExecutionCount() > 0 ? "E" : "Re-e", instance.getTaskName()));

      boolean do_commit = true;
      List<String> p_args = constructArgs(instance, do_commit);

      LSFBackEnd back_end = configureBackend(instance);

      LSFClusterCall call =
          back_end.new_call_instance(
              String.format(
                  "%s--%s--%s",
                  instance.getProcessName(), instance.getProcessId(), instance.getTaskName()),
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
                instance.getTaskName(), new ExternalCallException(call).toString());

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
