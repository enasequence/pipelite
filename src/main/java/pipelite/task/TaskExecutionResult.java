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
package pipelite.task;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import lombok.*;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;

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
