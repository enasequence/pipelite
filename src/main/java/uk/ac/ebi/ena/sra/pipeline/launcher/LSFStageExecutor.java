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

import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
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
  private LSFExecutorConfig config;
  private final int lsf_memory_limit;
  private final int cpu_cores;

  public LSFStageExecutor(
      String pipeline_name,
      TaskExecutionResultExceptionResolver resolver,
      int lsf_memory_limit,
      int cpu_cores,
      LSFExecutorConfig config) {
    this(
        pipeline_name,
        resolver,
        lsf_memory_limit,
        cpu_cores,
        DefaultConfiguration.CURRENT.getConfigPrefixName(),
        DefaultConfiguration.CURRENT.getConfigSourceName(),
        DefaultConfiguration.CURRENT.getPropertiesPass(),
        config);
  }

  LSFStageExecutor(
      String pipeline_name,
      TaskExecutionResultExceptionResolver resolver,
      int lsf_memory_limit,
      int cpu_cores,
      String config_prefix_name,
      String config_source_name,
      String[] properties_pass,
      LSFExecutorConfig config) {
    super(pipeline_name, resolver);
    this.internalError = resolver.internalError();
    this.config_prefix_name = config_prefix_name;
    this.config_source_name = config_source_name;

    this.lsf_memory_limit = lsf_memory_limit;
    this.cpu_cores = cpu_cores;
    this.properties_pass = properties_pass;

    this.config = config;
  }

  public void reset(StageInstance instance) {
    instance.setExecutionInstance(new ExecutionInstance());
  }

  public void configure(LSFExecutorConfig params) {
    if (null != params) this.config = params;
  }

  public String[] getPropertiesPass() {
    return properties_pass;
  }

  private String[] mergePropertiesPass(String[] pp1, String[] pp2) {
    Set<String> set1 = new HashSet<>(Arrays.asList(pp1));
    Set<String> set2 = new HashSet<>(Arrays.asList(pp2));
    set1.addAll(set2);
    return set1.toArray(new String[set1.size()]);
  }

  private List<String> constructArgs(StageInstance instance, boolean commit) {
    List<String> p_args = new ArrayList<>();

    p_args.add("-XX:+UseSerialGC");

    int lsf_memory_limit = instance.getMemoryLimit();
    if (lsf_memory_limit <= 0) {
      lsf_memory_limit = this.lsf_memory_limit;
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
    p_args.add(instance.getProcessID());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_STAGE);
    p_args.add(instance.getStageName());

    if (commit)
      p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_FORCE_COMMIT);

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ENABLED);
    p_args.add(Boolean.toString(instance.isEnabled()).toLowerCase());

    p_args.add(uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_EXEC_COUNT);
    p_args.add(Long.toString(instance.getExecutionCount()));

    return p_args;
  }

  private LSFBackEnd configureBackend(StageInstance instance) {
    int mem = instance.getMemoryLimit();
    if (mem <= 0) {
      mem = lsf_memory_limit;
      if (mem <= 0) {
        log.warn(
            "Setting LSF memory limit to default value "
                + LSF_JVM_MEMORY_DELTA_MB
                + LSF_JVM_MEMORY_OVERHEAD_MB
                + "MB.");
        mem = LSF_JVM_MEMORY_DELTA_MB + LSF_JVM_MEMORY_OVERHEAD_MB;
      }
    }
    int cpu = instance.getCPUCores();
    if (cpu <= 0) {
      cpu = cpu_cores;
      if (cpu <= 0) {
        log.warn("Setting CPU cores count to default value 1");
        cpu = 1;
      }
    }
    int mem_res = config.getLSFMemoryReservationTimeout();
    if (mem_res <= 0) {
      mem_res = LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES;
      log.warn(
          "Setting LSF memory timeout to default value "
              + LSF_JVM_MEMORY_RESERVATION_TIMEOUT_DEFAULT_MINUTES
              + "min.");
    }

    LSFBackEnd back_end = new LSFBackEnd(config.getLsfQueue(), mem, mem_res, cpu);
    back_end.setOutputFolderPath(Paths.get(config.getLsfOutputPath()));
    return back_end;
  }

  public void execute(StageInstance instance) {
    if (TaskExecutionState.ACTIVE_TASK == can_execute(instance)) {
      log.info(
          String.format(
              "%sxecuting stage %s",
              instance.getExecutionCount() > 0 ? "E" : "Re-e", instance.getStageName()));

      boolean do_commit = true;
      List<String> p_args = constructArgs(instance, do_commit);

      LSFBackEnd back_end = configureBackend(instance);

      LSFClusterCall call =
          back_end.new_call_instance(
              String.format(
                  "%s--%s--%s",
                  instance.getPipelineName(), instance.getProcessID(), instance.getStageName()),
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
                instance.getStageName(), new ExternalCallException(call).toString());

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

  @Override
  public Class<? extends ExecutorConfig> getConfigClass() {
    return LSFExecutorConfig.class;
  }

  @Override
  public <T extends ExecutorConfig> void configure(T params) {
    configure((LSFExecutorConfig) params);
  }
}
