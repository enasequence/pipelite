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
package pipelite.executor;

import lombok.Builder;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.Application;
import pipelite.instance.TaskInstance;
import pipelite.task.TaskExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor;

import java.util.ArrayList;
import java.util.List;

@Flogger
@Value
@Builder
public class SystemCallInternalTaskExecutor implements TaskExecutor {

  public TaskExecutionResult execute(TaskInstance taskInstance) {
    SystemCallTaskExecutor systemCallTaskExecutor =
        SystemCallTaskExecutor.builder()
            .executable("java")
            .arguments(getArguments(taskInstance))
            .resolver(
                (taskInstance1, exitCode) ->
                    taskInstance1.getResolver().serializer().deserialize(exitCode))
            .build();
    return systemCallTaskExecutor.execute(taskInstance);
  }

  public static List<String> getArguments(TaskInstance instance) {
    List<String> args = new ArrayList<>();

    Integer memory = instance.getTaskParameters().getMemory();

    if (memory != null && memory > 0) {
      args.add(String.format("-Xmx%dM", memory));
    }

    args.addAll(instance.getTaskParameters().getEnvAsJavaSystemPropertyOptions());

    args.add("-cp");
    args.add(System.getProperty("java.class.path"));
    args.add(InternalTaskExecutor.class.getName());

    // Arguments.
    args.add(instance.getProcessName());
    args.add(instance.getProcessId());
    args.add(instance.getTaskName());
    args.add(instance.getExecutor().getClass().getName());
    args.add(instance.getResolver().getClass().getName());

    return args;
  }
}
