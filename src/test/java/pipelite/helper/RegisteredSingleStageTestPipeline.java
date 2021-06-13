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
package pipelite.helper;

import static org.assertj.core.api.Assertions.assertThat;

import pipelite.service.StageService;

public abstract class RegisteredSingleStageTestPipeline<T extends RegisteredSingleStageTestPipeline>
    extends RegisteredTestPipeline<T> {

  private final String stageName = "STAGE";

  public RegisteredSingleStageTestPipeline(
      TestType testType,
      int immediateRetries,
      int maximumRetries,
      AssertSubmittedStageEntity<T> assertSubmittedStageEntity,
      AssertCompletedStageEntity<T> assertCompletedStageEntity) {
    super(
        testType,
        immediateRetries,
        maximumRetries,
        assertSubmittedStageEntity,
        assertCompletedStageEntity);
  }

  public String stageName() {
    return stageName;
  }

  @Override
  public final void assertSubmittedStageEntities(
      StageService stageService, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    for (String processId : configuredProcessIds()) {
      assertSubmittedStageEntity()
          .assertSubmittedStageEntity(stageService, pipelineName(), processId, stageName(), this);
    }
  }

  @Override
  public final void assertCompletedStageEntities(
      StageService stageService, int expectedProcessCnt) {
    assertThat(expectedProcessCnt).isEqualTo(configuredProcessCount());
    for (String processId : configuredProcessIds()) {
      assertCompletedStageEntity()
          .assertCompletedStageEntity(stageService, pipelineName(), processId, stageName(), this);
    }
  }
}
