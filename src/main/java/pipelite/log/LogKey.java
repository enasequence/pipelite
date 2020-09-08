/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.log;

import com.google.common.flogger.MetadataKey;
import pipelite.process.ProcessExecutionState;
import pipelite.task.TaskExecutionResultType;

public class LogKey {

  // Suppresses default constructor, ensuring non-instantiability.
  private LogKey() {}

  public static final MetadataKey<String> SERVICE_NAME =
      MetadataKey.single("service_name", String.class);

  public static final MetadataKey<String> LAUNCHER_NAME =
      MetadataKey.single("launcher_name", String.class);

  public static final MetadataKey<String> PROCESS_NAME =
      MetadataKey.single("process_name", String.class);

  public static final MetadataKey<String> PROCESS_ID =
      MetadataKey.single("process_id", String.class);

  public static final MetadataKey<ProcessExecutionState> PROCESS_STATE =
      MetadataKey.single("process_state", ProcessExecutionState.class);

  public static final MetadataKey<ProcessExecutionState> NEW_PROCESS_STATE =
      MetadataKey.single("new_process_state", ProcessExecutionState.class);

  public static final MetadataKey<String> TASK_NAME = MetadataKey.single("task_name", String.class);

  public static final MetadataKey<TaskExecutionResultType> TASK_EXECUTION_RESULT_TYPE =
      MetadataKey.single("task_execution_result_type", TaskExecutionResultType.class);

  public static final MetadataKey<Integer> PROCESS_EXECUTION_COUNT =
      MetadataKey.single("process_execution_count", Integer.class);

  public static final MetadataKey<Integer> TASK_EXECUTION_COUNT =
      MetadataKey.single("task_execution_count", Integer.class);

  public static final MetadataKey<String> TASK_EXECUTOR_CLASS_NAME =
      MetadataKey.single("task_executor_class_name", String.class);

  public static final MetadataKey<Integer> EXIT_CODE =
      MetadataKey.single("exit_code", Integer.class);
}
