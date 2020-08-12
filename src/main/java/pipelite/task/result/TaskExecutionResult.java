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

public interface TaskExecutionResult {

  TaskExecutionResultType getTaskExecutionResultType();

  default boolean isError() {
    return getTaskExecutionResultType().isError();
  }

  default boolean isPermanentError() {
    return getTaskExecutionResultType() == TaskExecutionResultType.PERMANENT_ERROR;
  }

  default boolean isTransientError() {
    return getTaskExecutionResultType() == TaskExecutionResultType.TRANSIENT_ERROR;
  }

  byte getExitCode();

  Class<? extends Throwable> getCause(); // throwable that can cause the status. unique.

  String getMessage(); // commit message. unique.
}
