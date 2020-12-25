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
package pipelite.schedule;

import java.util.ArrayList;
import java.util.List;
import pipelite.json.Json;

public class ScheduleExecutionHistory {

  private static int MAX_EXECUTION_HISTORY = 100;

  private List<ScheduleExecution> executions = new ArrayList<>();

  public List<ScheduleExecution> getExecutions() {
    return executions;
  }

  public void addExecution(ScheduleExecution execution) {
    executions.add(execution);
    if (executions.size() > MAX_EXECUTION_HISTORY) {
      executions = executions.subList(executions.size() - MAX_EXECUTION_HISTORY, executions.size());
    }
  }

  /**
   * Serializes the execution history to json. Returns null if serialization fails.
   *
   * @return serialized execution history or null if serialization fails
   */
  public String serialize() {
    return Json.serializeSafely(this);
  }

  /**
   * Deserializes the executor from json. Returns null if deserialization fails.
   *
   * @param json the json string
   * @return deserialized execution history or null if deserialization fails
   */
  public static ScheduleExecutionHistory deserialize(String json) {
    return Json.deserializeSafely(json, ScheduleExecutionHistory.class);
  }
}
