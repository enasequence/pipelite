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
import lombok.extern.flogger.Flogger;
import pipelite.json.Json;

@Getter
@Flogger
public class StageExecutorResult {

  private StageExecutorState executorState;
  private String stageLog;
  private ErrorType errorType;

  private final Map<String, String> attributes = new HashMap<>();

  public StageExecutorResult(StageExecutorState executorState) {
    if (executorState == null) {
      throw new IllegalArgumentException("Missing executor state");
    }
    this.executorState = executorState;
  }

  public static StageExecutorResult submitted() {
    return new StageExecutorResult(StageExecutorState.SUBMITTED);
  }

  public static StageExecutorResult active() {
    return new StageExecutorResult(StageExecutorState.ACTIVE);
  }

  public static StageExecutorResult success() {
    return new StageExecutorResult(StageExecutorState.SUCCESS);
  }

  public static StageExecutorResult error() {
    return new StageExecutorResult(StageExecutorState.ERROR);
  }

  public static StageExecutorResult from(StageExecutorState stageExecutorState) {
    return new StageExecutorResult(stageExecutorState);
  }

  public boolean isSubmitted() {
    return executorState == StageExecutorState.SUBMITTED;
  }

  public boolean isActive() {
    return executorState == StageExecutorState.ACTIVE;
  }

  public boolean isSuccess() {
    return executorState == StageExecutorState.SUCCESS;
  }

  public boolean isError() {
    return executorState == StageExecutorState.ERROR;
  }

  public StageExecutorResult setSubmitted() {
    executorState = StageExecutorState.SUBMITTED;
    errorType = null;
    return this;
  }

  /**
   * Creates an internal error. The exception stack trace is written to the stage log.
   *
   * @param ex the exception
   * @return the stage execution result
   */
  public static StageExecutorResult internalError(Exception ex) {
    StageExecutorResult result = error().setInternalError();
    StringWriter str = new StringWriter();
    ex.printStackTrace(new PrintWriter(str));
    result.setStageLog(str.toString());
    return result;
  }

  /**
   * Creates an timeout error that is a type of permanent error.
   *
   * @return the stage execution result
   */
  public static StageExecutorResult timeoutError() {
    return error().setTimeoutError();
  }

  /**
   * Creates an interrupted error.
   *
   * @return the stage execution result
   */
  public static StageExecutorResult interruptedError() {
    return error().setInterruptedError();
  }

  /**
   * Creates a permanent error.
   *
   * @return the stage execution result
   */
  public static StageExecutorResult permanentError() {
    return error().setPermanentError();
  }

  public StageExecutorResult setErrorType(ErrorType errorType) {
    executorState = StageExecutorState.ERROR;
    this.errorType = errorType;
    return this;
  }

  public StageExecutorResult setInternalError() {
    return setErrorType(ErrorType.INTERNAL_ERROR);
  }

  public StageExecutorResult setTimeoutError() {
    return setErrorType(ErrorType.TIMEOUT_ERROR);
  }

  public StageExecutorResult setInterruptedError() {
    return setErrorType(ErrorType.INTERRUPTED_ERROR);
  }

  public StageExecutorResult setPermanentError() {
    return setErrorType(ErrorType.PERMANENT_ERROR);
  }

  public void setStageLog(String stageLog) {
    this.stageLog = stageLog;
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

  public ErrorType getErrorType() {
    if (executorState == StageExecutorState.ERROR && errorType == null) {
      return ErrorType.EXECUTION_ERROR;
    }
    return errorType;
  }

  public boolean isErrorType(ErrorType errorType) {
    return this.errorType == errorType;
  }

  public static boolean isExecutableErrorType(ErrorType errorType) {
    if (errorType == null) {
      return true;
    }
    return !errorType.equals(ErrorType.PERMANENT_ERROR)
        && !errorType.equals(ErrorType.TIMEOUT_ERROR);
  }

  public String attributesJson() {
    if (attributes.isEmpty()) {
      return null;
    }
    return Json.serializeSafely(attributes);
  }
}
