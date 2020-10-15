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

public class StageExecutionResultExitCode {

  private StageExecutionResultExitCode() {}

  public static int EXIT_CODE_SUCCESS = 0;
  public static int EXIT_CODE_ERROR = 1;

  public static StageExecutionResult resolve(int exitCode) {
    if (exitCode == EXIT_CODE_SUCCESS) {
      return StageExecutionResult.success();
    }
    return StageExecutionResult.error();
  }

  public static int serialize(StageExecutionResult result) {
    if (result.isSuccess()) {
      return EXIT_CODE_SUCCESS;
    }
    return EXIT_CODE_ERROR;
  }

  public static StageExecutionResult deserialize(int exitCode) {
    if (exitCode == EXIT_CODE_SUCCESS) {
      return StageExecutionResult.success();
    }
    return StageExecutionResult.error();
  }
}