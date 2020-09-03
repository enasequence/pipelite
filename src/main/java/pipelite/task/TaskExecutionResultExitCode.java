package pipelite.task;

public class TaskExecutionResultExitCode {

  private TaskExecutionResultExitCode() {}

  public static int EXIT_CODE_SUCCESS = 0;
  public static int EXIT_CODE_ERROR = 1;

  public static TaskExecutionResult resolve(int exitCode) {
    if (exitCode == EXIT_CODE_SUCCESS) {
      return TaskExecutionResult.success();
    }
    return TaskExecutionResult.error();
  }

  public static int serialize(TaskExecutionResult result) {
    if (result.isSuccess()) {
      return EXIT_CODE_SUCCESS;
    }
    return EXIT_CODE_ERROR;
  }

  public static TaskExecutionResult deserialize(int exitCode) {
    if (exitCode == EXIT_CODE_SUCCESS) {
      return TaskExecutionResult.success();
    }
    return TaskExecutionResult.error();
  }
}
