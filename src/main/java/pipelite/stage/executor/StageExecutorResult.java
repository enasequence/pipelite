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
package pipelite.stage.executor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import lombok.*;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;

@Getter
@Setter
@Flogger
public class StageExecutorResult {

  private StageExecutorResultType resultType;
  private String stageLog;

  /** True if an internal error has been registered. */
  private boolean internalError;

  /** True if an timeout error has been registered. */
  private boolean timeoutError;

  private final Map<String, String> attributes = new HashMap<>();

  public StageExecutorResult(StageExecutorResultType resultType) {
    if (resultType == null) {
      throw new IllegalArgumentException("Missing result type");
    }
    this.resultType = resultType;
  }

  public boolean isActive() {
    return StageExecutorResultType.isActive(resultType);
  }

  public boolean isSuccess() {
    return StageExecutorResultType.isSuccess(resultType);
  }

  public boolean isError() {
    return StageExecutorResultType.isError(resultType);
  }

  public static StageExecutorResult active() {
    return new StageExecutorResult(StageExecutorResultType.ACTIVE);
  }

  public static StageExecutorResult success() {
    return new StageExecutorResult(StageExecutorResultType.SUCCESS);
  }

  public static StageExecutorResult error() {
    return new StageExecutorResult(StageExecutorResultType.ERROR);
  }

  /**
   * Creates an internal error. The exception stack trace is written to the stage log.
   *
   * @param ex the exception
   * @return the stage execution result
   */
  public static StageExecutorResult internalError(Exception ex) {
    StageExecutorResult result = error();
    result.internalError = true;
    StringWriter str = new StringWriter();
    ex.printStackTrace(new PrintWriter(str));
    result.setStageLog(str.toString());
    return result;
  }

  /**
   * Creates an timeout error.
   *
   * @return the stage execution result
   */
  public static StageExecutorResult timeoutError() {
    StageExecutorResult result = error();
    result.timeoutError = true;
    return result;
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

  public String attributesJson() {
    if (attributes.isEmpty()) {
      return null;
    }
    return Json.serializeSafely(attributes);
  }
}
