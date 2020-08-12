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
package pipelite.task.result;

import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.TaskExecutionResultType;

public enum ExecutionResults implements TaskExecutionResult {
  OK(null, 0, null, TaskExecutionResultType.SUCCESS),
  SKIPPED("SKIPPED", 51, Exception.class, TaskExecutionResultType.SUCCESS),
  UNKNOWN_FAILURE("UNKNOWN_FAILURE", 53, Throwable.class, TaskExecutionResultType.TRANSIENT_ERROR);

  ExecutionResults(String message, int exit_code, Class<?> cause, TaskExecutionResultType type) {
    this.message = message;
    this.type = type;
    this.exit_code = (byte) exit_code;
    this.cause = cause;
  }

  final String message;
  final TaskExecutionResultType type;
  final byte exit_code;
  final Class<?> cause;

  @Override
  public byte getExitCode() {
    return exit_code;
  }

  @Override
  public Class<Throwable> getCause() {
    return (Class<Throwable>) cause;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public TaskExecutionResultType getTaskExecutionResultType() {
    return type;
  }
}
