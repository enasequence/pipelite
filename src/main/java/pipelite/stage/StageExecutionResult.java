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
package pipelite.stage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import lombok.*;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;

@Data
@Flogger
public class StageExecutionResult {

  private StageExecutionResultType resultType;
  private String stdout;
  private String stderr;

  public StageExecutionResult(StageExecutionResultType resultType) {
    this.resultType = resultType;
  }

  @EqualsAndHashCode.Exclude private final Map<String, String> attributes = new HashMap<>();

  public static final String HOST = "host";
  public static final String EXEC_HOST = "execution host";
  public static final String CPU_TIME = "cpu time";
  public static final String MAX_MEM = "max mem";
  public static final String AVG_MEM = "avg mem";
  public static final String MESSAGE = "message";
  public static final String EXCEPTION = "exception";
  public static final String COMMAND = "command";
  public static final String EXIT_CODE = "exit code";

  public boolean isActive() {
    if (resultType == null) {
      return false;
    }
    return StageExecutionResultType.isActive(resultType);
  }

  public boolean isSuccess() {
    if (resultType == null) {
      return false;
    }
    return StageExecutionResultType.isSuccess(resultType);
  }

  public boolean isError() {
    if (resultType == null) {
      return false;
    }
    return StageExecutionResultType.isError(resultType);
  }

  public static StageExecutionResult active() {
    return new StageExecutionResult(StageExecutionResultType.ACTIVE);
  }

  public static StageExecutionResult success() {
    return new StageExecutionResult(StageExecutionResultType.SUCCESS);
  }

  public static StageExecutionResult error() {
    return new StageExecutionResult(StageExecutionResultType.ERROR);
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
    StringWriter stackTrace = new StringWriter();
    PrintWriter pw = new PrintWriter(stackTrace);
    value.printStackTrace(pw);
    addAttribute(StageExecutionResult.EXCEPTION, stackTrace.toString());
  }

  public String attributesJson() {
    if (attributes.isEmpty()) {
      return null;
    }
    return Json.serializeSafely(attributes);
  }
}
