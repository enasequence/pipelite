package pipelite.log;

import com.google.common.flogger.MetadataKey;
import pipelite.process.ProcessExecutionState;
import pipelite.task.result.TaskExecutionResultType;

public abstract class LogKey {
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

  public static final MetadataKey<String> STAGE_NAME =
      MetadataKey.single("stage_name", String.class);

  public static final MetadataKey<TaskExecutionResultType> TASK_EXECUTION_RESULT_TYPE =
      MetadataKey.single("task_execution_result_type", TaskExecutionResultType.class);

  public static final MetadataKey<String> TASK_EXECUTION_RESULT =
      MetadataKey.single("task_execution_result", String.class);

  public static final MetadataKey<Integer> PROCESS_EXECUTION_COUNT =
      MetadataKey.single("process_execution_count", Integer.class);

  public static final MetadataKey<Integer> TASK_EXECUTION_COUNT =
      MetadataKey.single("task_execution_count", Integer.class);
}
