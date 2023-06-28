/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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
import org.springframework.util.Assert;
import pipelite.json.Json;

public class StageExecutorResult {

  private StageExecutorState state;

  private String stdOut;

  private String stdErr;

  private final Map<String, String> attributes = new HashMap<>();

  private StageExecutorResult(StageExecutorState state) {
    this.state = state;
  }

  public static StageExecutorResult create(StageExecutorState state) {
    Assert.notNull(state, "Missing state");
    return new StageExecutorResult(state);
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

  public static StageExecutorResult executionError() {
    return new StageExecutorResult(StageExecutorState.EXECUTION_ERROR);
  }

  public static StageExecutorResult timeoutError() {
    return new StageExecutorResult(StageExecutorState.TIMEOUT_ERROR);
  }

  public static StageExecutorResult lostError() {
    return new StageExecutorResult(StageExecutorState.LOST_ERROR);
  }

  public static StageExecutorResult internalError() {
    return new StageExecutorResult(StageExecutorState.INTERNAL_ERROR);
  }

  public boolean isSubmitted() {
    return state == StageExecutorState.SUBMITTED;
  }

  public boolean isActive() {
    return state == StageExecutorState.ACTIVE;
  }

  public boolean isSuccess() {
    return state == StageExecutorState.SUCCESS;
  }

  public boolean isError() {
    return state.isError();
  }

  public boolean isExecutionError() {
    return state == StageExecutorState.EXECUTION_ERROR;
  }

  public boolean isTimeoutError() {
    return state == StageExecutorState.TIMEOUT_ERROR;
  }

  public boolean isLostError() {
    return state == StageExecutorState.LOST_ERROR;
  }

  public boolean isInternalError() {
    return state == StageExecutorState.INTERNAL_ERROR;
  }

  public boolean isCompleted() {
    return state.isCompleted();
  }

  public StageExecutorResult state(StageExecutorState state) {
    this.state = state;
    return this;
  }

  public StageExecutorResult stdOut(String stdOut) {
    this.stdOut = stdOut;
    return this;
  }

  public StageExecutorResult stdErr(String stdErr) {
    this.stdErr = stdErr;
    return this;
  }

  public StageExecutorResult stageLog(StageExecutorResult result) {
    if (result != null) {
      this.stdOut = result.stdOut;
      this.stdErr = result.stdErr;
    }
    return this;
  }

  public StageExecutorResult stageLog(Exception ex) {
    StringWriter str = new StringWriter();
    ex.printStackTrace(new PrintWriter(str));
    this.stdErr = str.toString();
    return this;
  }

  public StageExecutorResult attribute(String key, Object value) {
    if (key == null || value == null) {
      return this;
    }
    attributes.put(key, value.toString());
    return this;
  }

  public StageExecutorResult attributes(StageExecutorResult result) {
    if (result != null) {
      this.attributes.clear();
      this.attributes.putAll(result.attributes);
    }
    return this;
  }

  public String exitCode() {
    return attributes.get(StageExecutorResultAttribute.EXIT_CODE);
  }

  public StageExecutorState state() {
    return state;
  }

  public String stdOut() {
    return stdOut;
  }

  public String stdErr() {
    return stdErr;
  }

  public String stageLog() {
    return (stdOut != null ? stdOut : "") + (stdErr != null ? stdErr : "");
  }

  public String attribute(String value) {
    return attributes.get(value);
  }

  public String attributesJson() {
    if (attributes.isEmpty()) {
      return null;
    }
    return Json.serializeSafely(attributes);
  }
}
