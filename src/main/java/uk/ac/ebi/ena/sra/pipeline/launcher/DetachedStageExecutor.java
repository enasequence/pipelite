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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import pipelite.task.executor.AbstractTaskExecutor;
import pipelite.task.instance.LatestTaskExecution;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.executors.DetachedExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

public class DetachedStageExecutor extends AbstractTaskExecutor {
  private final String config_prefix_name;
  private final String config_source_name;
  ExecutionInfo info;
  protected final ExternalCallBackEnd back_end = new SimpleBackEnd();
  private final String[] properties_pass;

  public DetachedStageExecutor(
      String pipeline_name,
      TaskExecutionResultExceptionResolver resolver,
      String config_prefix_name,
      String config_source_name,
      String[] properties_pass) {
    super(pipeline_name, resolver);
    this.config_prefix_name = config_prefix_name;
    this.config_source_name = config_source_name;
    this.properties_pass = properties_pass;
  }

  public void reset(TaskInstance instance) {
    instance.setLatestTaskExecution(new LatestTaskExecution());
  }

  private String[] mergePropertiesPass(String[] pp1, String[] pp2) {
    Set<String> set1 = new HashSet<>(Arrays.asList(pp1));
    Set<String> set2 = new HashSet<>(Arrays.asList(pp2));
    set1.addAll(set2);
    return set1.toArray(new String[set1.size()]);
  }

  private List<String> constructArgs(TaskInstance instance, boolean commit) {
    List<String> p_args = new ArrayList<>();

    int memory_limit = instance.getMemory();

    if (0 < memory_limit) // TODO check
    p_args.add(String.format("-Xmx%dM", memory_limit));

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

    return p_args;
  }

  public void execute(TaskInstance instance) {
    if (TaskExecutionState.ACTIVE_TASK == getTaskExecutionState(instance)) {
      log.info(
          String.format(
              "%sxecuting stage %s",
              0 == instance.getExecutionCount() ? "E" : "Re-e", instance.getTaskName()));

      boolean do_commit = true;
      List<String> p_args = constructArgs(instance, do_commit);
      ExternalCall ec =
          back_end.new_call_instance(
              String.format(
                  "%s~%s~%s",
                  PIPELINE_NAME, // TODO: get from instance
                  instance.getProcessId(),
                  instance.getTaskName()),
              "java",
              p_args.toArray(new String[p_args.size()]));

      log.info(ec.getCommandLine());

      ec.execute();

      fillExecutionInfo(ec);

      String print_msg =
          String.format(
              "Finished execution of stage %s\n%s",
              instance.getTaskName(), new ExternalCallException(ec).toString());

      log.info(print_msg);
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
    info.setThrowable(new ExternalCallException(ec));
  }

  @Override
  public ExecutionInfo get_info() {
    return info;
  }


  @Override
  public Class<? extends ExecutorConfig> getConfigClass() {
    return DetachedExecutorConfig.class;
  }

  @Override
  public void configure(ExecutorConfig rc) {
  }

  public String[] getPropertiesPass() {
    return properties_pass;
  }
}
