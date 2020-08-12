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
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.DetachedExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

public class DetachedStageExecutor extends AbstractStageExecutor {
  private boolean do_commit = true;
  private String config_prefix_name;
  private String config_source_name;
  ExecutionInfo info;
  protected ExternalCallBackEnd back_end = new SimpleBackEnd();
  private String[] properties_pass;

  DetachedExecutorConfig config;

  public DetachedStageExecutor(String pipeline_name, ResultTranslator translator) {
    this(
        pipeline_name,
        translator,
        DefaultConfiguration.currentSet().getConfigPrefixName(),
        DefaultConfiguration.currentSet().getConfigSourceName(),
        DefaultConfiguration.CURRENT.getPropertiesPass());
  }

  public DetachedStageExecutor(
      String pipeline_name,
      ResultTranslator translator,
      String config_prefix_name,
      String config_source_name,
      String[] properties_pass) {
    super(pipeline_name, translator);
    this.config_prefix_name = config_prefix_name;
    this.config_source_name = config_source_name;
    this.properties_pass = properties_pass;
  }

  public void reset(StageInstance instance) {
    instance.setExecutionInstance(new ExecutionInstance());
  }

  private String[] mergePropertiesPass(String[] pp1, String[] pp2) {
    Set<String> set1 = new HashSet<>(Arrays.asList(pp1));
    Set<String> set2 = new HashSet<>(Arrays.asList(pp2));
    set1.addAll(set2);
    return set1.toArray(new String[set1.size()]);
  }

  private List<String> constructArgs(StageInstance instance, boolean commit) {
    List<String> p_args = new ArrayList<String>();

    int memory_limit = instance.getMemoryLimit();

    if (0 < memory_limit) // TODO check
    p_args.add(String.format("-Xmx%dM", memory_limit));

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

    return p_args;
  }

  public void execute(StageInstance instance) {
    if (EvalResult.StageTransient == can_execute(instance)) {
      log.info(
          String.format(
              "%sxecuting stage %s",
              0 == instance.getExecutionCount() ? "E" : "Re-e", instance.getStageName()));

      List<String> p_args = constructArgs(instance, do_commit);
      ExternalCall ec =
          back_end.new_call_instance(
              String.format(
                  "%s~%s~%s",
                  PIPELINE_NAME, // TODO: get from instance
                  instance.getProcessID(),
                  instance.getStageName()),
              "java",
              p_args.toArray(new String[p_args.size()]));

      log.info(ec.getCommandLine());

      ec.execute();

      fillExecutionInfo(ec);

      String print_msg =
          String.format(
              "Finished execution of stage %s\n%s",
              instance.getStageName(), new ExternalCallException(ec).toString());

      log.info(print_msg);
    }
  }

  private void fillExecutionInfo(ExternalCall ec) {
    info = new ExecutionInfo();
    info.setCommandline(ec.getCommandLine());
    info.setStdout(ec.getStdout());
    info.setStderr(ec.getStderr());
    info.setExitCode(Integer.valueOf(ec.getExitCode()));
    info.setHost(ec.getHost());
    info.setPID(ec.getPID());
    info.setThrowable(new ExternalCallException(ec));
  }

  @Override
  public ExecutionInfo get_info() {
    return info;
  }

  @Override
  public void setClientCanCommit(boolean do_commit) {
    this.do_commit = do_commit;
  }

  @Override
  public boolean getClientCanCommit() {
    return this.do_commit;
  }

  @Override
  public Class<? extends ExecutorConfig> getConfigClass() {
    return DetachedExecutorConfig.class;
  }

  @Override
  public void configure(ExecutorConfig rc) {
    DetachedExecutorConfig params = (DetachedExecutorConfig) rc;
    if (null != params) {
      this.config = params;
    }
  }

  public String[] getPropertiesPass() {
    return properties_pass;
  }
}
