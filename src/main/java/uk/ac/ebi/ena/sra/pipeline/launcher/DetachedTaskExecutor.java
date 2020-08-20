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
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.instance.TaskInstance;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;

@Slf4j
public class DetachedTaskExecutor extends AbstractTaskExecutor {
  ExecutionInfo info;
  protected final ExternalCallBackEnd back_end = new SimpleBackEnd();

  public DetachedTaskExecutor(
      ProcessConfiguration processConfiguration, TaskConfiguration taskConfiguration) {
    super(processConfiguration, taskConfiguration);
  }

  public void reset(TaskInstance instance) {
    instance.getPipeliteStage().resetExecution();
  }

  private List<String> constructArgs(TaskInstance instance, boolean commit) {
    List<String> p_args = new ArrayList<>();

    Integer memory = instance.getMemory();

    if (memory != null && memory > 0) {
      p_args.add(String.format("-Xmx%dM", memory));
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

    return p_args;
  }

  public void execute(TaskInstance taskInstance) {
    log.info(
        String.format(
            "%sxecuting stage %s",
            0 == taskInstance.getPipeliteStage().getExecutionCount() ? "E" : "Re-e",
            taskInstance.getPipeliteStage().getStageName()));

    boolean do_commit = true;
    List<String> p_args = constructArgs(taskInstance, do_commit);
    ExternalCall ec =
        back_end.new_call_instance(
            String.format(
                "%s~%s~%s",
                taskInstance.getPipeliteProcess().getProcessName(),
                taskInstance.getPipeliteStage().getProcessId(),
                taskInstance.getPipeliteStage().getStageName()),
            "java",
            p_args.toArray(new String[0]));

    log.info(ec.getCommandLine());

    ec.execute();

    fillExecutionInfo(ec);

    String print_msg =
        String.format(
            "Finished execution of stage %s\n%s",
            taskInstance.getPipeliteStage().getStageName(),
            new ExternalCallException(ec).toString());

    log.info(print_msg);
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
}