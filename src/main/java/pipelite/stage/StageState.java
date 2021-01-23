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

public enum StageState {
  PENDING,
  ACTIVE,
  SUCCESS,
  ERROR;

  public static boolean isPending(StageState stageState) {
    return stageState == PENDING;
  }

  public static boolean isActive(StageState stageState) {
    return stageState == ACTIVE;
  }

  public static boolean isError(StageState stageState) {
    return stageState == ERROR;
  }

  public static boolean isSuccess(StageState stageState) {
    return stageState == SUCCESS;
  }
}
