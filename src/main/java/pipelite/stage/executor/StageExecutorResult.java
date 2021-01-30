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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;
import pipelite.stage.StageState;

@Getter
@Setter
@Flogger
public class StageExecutorResult {

  private StageState stageState;
  private String stageLog;

  /** True if an internal error has been registered. */
  private boolean internalError;

  /** True if an timeout error has been registered. */
  private boolean timeoutError;

  /** True if an interrupted error has been registered. */
  private boolean interruptedError;

  private final Map<String, String> attributes = new HashMap<>();

  public StageExecutorResult(StageState stageState) {
    if (stageState == null) {
      throw new IllegalArgumentException("Missing stage state");
    }
    this.stageState = stageState;
  }

  public boolean isActive() {
    return StageState.isActive(stageState);
  }

  public boolean isSuccess() {
    return StageState.isSuccess(stageState);
  }

  public boolean isError() {
    return StageState.isError(stageState);
  }

  public static StageExecutorResult active() {
    return new StageExecutorResult(StageState.ACTIVE);
  }

  public static StageExecutorResult success() {
    return new StageExecutorResult(StageState.SUCCESS);
  }

  public static StageExecutorResult error() {
    return new StageExecutorResult(StageState.ERROR);
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

  /**
   * Creates an interrupted error.
   *
   * @return the stage execution result
   */
  public static StageExecutorResult interruptedError() {
    StageExecutorResult result = error();
    result.interruptedError = true;
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
