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
package pipelite.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;
import pipelite.entity.StageEntity;
import pipelite.entity.field.ErrorType;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorState;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.test.configuration.PipeliteTestConfigWithServices;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=StageRepositoryTest"})
@DirtiesContext
@Transactional
class StageRepositoryTest {

  @Autowired StageRepository repository;

  @Test
  public void errorType() {
    assertThat(testNonPermanentErrorType(StageExecutorState.EXECUTION_ERROR).getErrorType())
        .isEqualTo(ErrorType.EXECUTION_ERROR);
    assertThat(testPermanentErrorType().getErrorType()).isEqualTo(ErrorType.PERMANENT_ERROR);
    assertThat(testNonPermanentErrorType(StageExecutorState.TIMEOUT_ERROR).getErrorType())
        .isEqualTo(ErrorType.TIMEOUT_ERROR);
    assertThat(testNonPermanentErrorType(StageExecutorState.MEMORY_ERROR).getErrorType())
        .isEqualTo(ErrorType.MEMORY_ERROR);
    assertThat(testNonPermanentErrorType(StageExecutorState.TERMINATED_ERROR).getErrorType())
        .isEqualTo(ErrorType.TERMINATED_ERROR);
    assertThat(testNonPermanentErrorType(StageExecutorState.LOST_ERROR).getErrorType())
        .isEqualTo(ErrorType.LOST_ERROR);
    assertThat(testNonPermanentErrorType(StageExecutorState.INTERNAL_ERROR).getErrorType())
        .isEqualTo(ErrorType.INTERNAL_ERROR);
  }

  private StageEntity testNonPermanentErrorType(StageExecutorState state) {
    StageEntity stageEntity =
        StageEntity.createExecution(
            PipeliteTestIdCreator.pipelineName(),
            PipeliteTestIdCreator.processId(),
            PipeliteTestIdCreator.stageName());
    stageEntity.startExecution();
    stageEntity.endExecution(StageExecutorResult.create(state), Collections.emptyList());
    return repository.save(stageEntity);
  }

  private StageEntity testPermanentErrorType() {
    StageEntity stageEntity =
        StageEntity.createExecution(
            PipeliteTestIdCreator.pipelineName(),
            PipeliteTestIdCreator.processId(),
            PipeliteTestIdCreator.stageName());
    stageEntity.startExecution();
    StageExecutorResult result = StageExecutorResult.executionError();
    result.attribute(StageExecutorResultAttribute.EXIT_CODE, "1");
    stageEntity.endExecution(result, Arrays.asList(1));
    return repository.save(stageEntity);
  }
}
