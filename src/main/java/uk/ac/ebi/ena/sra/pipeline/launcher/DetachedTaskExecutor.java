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

import java.util.List;

import lombok.extern.flogger.Flogger;
import pipelite.configuration.TaskConfigurationEx;
import pipelite.executor.AbstractTaskExecutor;
import pipelite.instance.TaskInstance;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;

import static uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor.callInternalTaskExecutor;

@Flogger
public class DetachedTaskExecutor extends AbstractTaskExecutor {

  protected final ExternalCallBackEnd back_end = new SimpleBackEnd();

  public DetachedTaskExecutor(TaskConfigurationEx taskConfiguration) {
    super(taskConfiguration);
  }

  public ExecutionInfo execute(TaskInstance taskInstance) {

    List<String> cmd = callInternalTaskExecutor(taskInstance);
    ExternalCall ec =
        back_end.new_call_instance(
            String.format(
                // TODO: job name
                "%s~%s~%s",
                taskInstance.getProcessName(),
                taskInstance.getProcessId(),
                taskInstance.getTaskName()),
            "java",
            cmd.toArray(new String[0]));

    log.atInfo().log(ec.getCommandLine());

    ec.execute();

    ExecutionInfo info = new ExecutionInfo();
    info.setCommandline(ec.getCommandLine());
    info.setStdout(ec.getStdout());
    info.setStderr(ec.getStderr());
    info.setExitCode(ec.getExitCode());
    info.setHost(ec.getHost());
    info.setPID(ec.getPID());
    info.setThrowable(new ExternalCallException(ec));
    return info;
  }
}
