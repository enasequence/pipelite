package pipelite.task;

import lombok.*;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@Data
@Flogger
public class TaskExecutionResult {

  private TaskExecutionResultType resultType;
  private String stdout;
  private String stderr;

  public TaskExecutionResult(@NonNull TaskExecutionResultType resultType) {
    this.resultType = resultType;
  }

  @EqualsAndHashCode.Exclude private final Map<String, String> attributes = new HashMap<>();

  public static final String HOST = "host";
  public static final String MESSAGE = "message";
  public static final String EXCEPTION = "exception";
  public static final String COMMAND = "command";
  public static final String EXIT_CODE = "exit code";

  public boolean isActive() {
    return resultType == TaskExecutionResultType.ACTIVE;
  }

  public boolean isSuccess() {
    return resultType == TaskExecutionResultType.SUCCESS;
  }

  public boolean isError() {
    return resultType.isError();
  }

  public static TaskExecutionResult active() {
    return new TaskExecutionResult(TaskExecutionResultType.ACTIVE);
  }

  public static TaskExecutionResult success() {
    return new TaskExecutionResult(TaskExecutionResultType.SUCCESS);
  }

  public static TaskExecutionResult error() {
    return new TaskExecutionResult(TaskExecutionResultType.ERROR);
  }

  public String getAttribute(String value) {
    return attributes.get(value);
  }

  public void addAttribute(String key, Object value) {
    if (key == null || value == null) {
      return;
    }
    attributes.put(key, value.toString());
  }

  public void addExceptionAttribute(Exception value) {
    if (value == null) {
      return;
    }
    PrintWriter pw = new PrintWriter(new StringWriter());
    value.printStackTrace(pw);
    addAttribute(TaskExecutionResult.EXCEPTION, pw.toString());
  }

  public String attributesJson() {
    if (attributes.isEmpty()) {
      return null;
    }
    return Json.serializeNullIfErrorOrEmpty(attributes);
  }
}
