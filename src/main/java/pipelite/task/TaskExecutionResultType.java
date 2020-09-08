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

public enum TaskExecutionResultType {
  NEW(false),
  ACTIVE(false),
  SUCCESS(false),
  ERROR(true);

  TaskExecutionResultType(boolean isError) {
    this.isError = isError;
  }

  private final boolean isError;

  public boolean isError() {
    return isError;
  }
}
