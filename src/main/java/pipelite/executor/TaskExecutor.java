/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import pipelite.task.TaskInstance;
import pipelite.task.TaskExecutionResult;

public interface TaskExecutor {

  TaskExecutor SUCCESS_EXECUTOR = new SuccessTaskExecutor();
  TaskExecutor PERMANENT_ERROR_EXECUTOR = new PermanentErrorTaskExecutor();

  TaskExecutionResult execute(TaskInstance instance);

  default TaskExecutionResult resume(TaskInstance instance) {
    return TaskExecutionResult.resumeTransientError();
  }

  static String serialize(TaskExecutor executor) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    try {
      return objectMapper.writeValueAsString(executor);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static TaskExecutor deserialize(String className, String data) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    try {
      return (TaskExecutor) objectMapper.readValue(data, Class.forName(className));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
