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
package pipelite.helper.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.entity.StageEntity;
import pipelite.helper.TestType;
import pipelite.service.StageService;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;

public class StageEntityTestHelper {
  private StageEntityTestHelper() {}

  private static StageEntity assertSubmittedExecutorStageEntity(
      StageService stageService, String pipelineName, String processId, String stageName) {

    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getStageState()).isEqualTo(StageState.ACTIVE);
    assertThat(stageEntity.getErrorType()).isNull();
    assertThat(stageEntity.getExecutionCount()).isZero();
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isNull();
    return stageEntity;
  }

  private static StageEntity assertCompletedExecutorStageEntity(
      StageService stageService,
      String pipelineName,
      String processId,
      String stageName,
      TestType testType,
      int immediateRetries,
      int maximumRetries) {

    StageState expectedStageState;
    ErrorType expectedErrorType = null;
    if (testType == TestType.PERMANENT_ERROR) {
      expectedStageState = StageState.ERROR;
      expectedErrorType = ErrorType.PERMANENT_ERROR;
    } else if (testType == TestType.NON_PERMANENT_ERROR) {
      expectedStageState = StageState.ERROR;
      expectedErrorType = ErrorType.EXECUTION_ERROR;
    } else {
      expectedStageState = StageState.SUCCESS;
    }
    int expectedExecutionCountBasedOnImmediateRetries =
        testType == TestType.NON_PERMANENT_ERROR ? immediateRetries + 1 : 1;
    int expectedExecutionCountBasedOnMaximumRetries =
        testType == TestType.NON_PERMANENT_ERROR ? maximumRetries + 1 : 1;

    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getStageState()).isEqualTo(expectedStageState);
    assertThat(stageEntity.getErrorType()).isEqualTo(expectedErrorType);
    assertThat(stageEntity.getExecutionCount())
        .isIn(
            expectedExecutionCountBasedOnImmediateRetries,
            expectedExecutionCountBasedOnMaximumRetries);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isAfterOrEqualTo(stageEntity.getStartTime());
    return stageEntity;
  }

  public static void assertCompletedTestExecutorStageEntity(
      TestType testType,
      StageService stageService,
      String pipelineName,
      String processId,
      String stageName,
      int immediateRetries,
      int maximumRetries) {

    StageEntity stageEntity =
        assertCompletedExecutorStageEntity(
            stageService,
            pipelineName,
            processId,
            stageName,
            testType,
            immediateRetries,
            maximumRetries);

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.TestExecutor");

    assertThat(stageEntity.getExecutorParams()).contains("\"maximumRetries\" : " + maximumRetries);
    assertThat(stageEntity.getExecutorParams())
        .contains("\"immediateRetries\" : " + immediateRetries);
  }

  private static void assertSimpleLsfExecutorStageEntity(
      List<Integer> permanentErrors,
      String cmd,
      int immediateRetries,
      int maximumRetries,
      StageEntity stageEntity,
      LsfTestConfiguration lsfTestConfiguration) {
    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");

    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + cmd + "\"");
    assertThat(stageEntity.getExecutorData()).contains("  \"jobId\" : \"");
    assertThat(stageEntity.getExecutorData()).contains("  \"outFile\" : \"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + maximumRetries
                + ",\n"
                + "  \"immediateRetries\" : "
                + immediateRetries
                + ",\n"
                + "  \"host\" : \""
                + lsfTestConfiguration.getHost()
                + "\",\n"
                + "  \"workDir\" : \""
                + lsfTestConfiguration.getWorkDir()
                + "\",\n"
                + "  \"saveLog\" : true,\n"
                + (permanentErrors != null && !permanentErrors.isEmpty()
                    ? "  \"logBytes\" : 1048576,\n"
                        + "  \"logTimeout\" : 10000,\n"
                        + "  \"permanentErrors\" : [ "
                        + String.join(
                            ", ",
                            permanentErrors.stream()
                                .map(i -> i.toString())
                                .collect(Collectors.toList()))
                        + " ]\n"
                    : "  \"logBytes\" : 1048576,\n" + "  \"logTimeout\" : 10000\n")
                + "}");
  }

  public static void assertSubmittedSimpleLsfExecutorStageEntity(
      StageService stageService,
      LsfTestConfiguration lsfTestConfiguration,
      String pipelineName,
      String processId,
      String stageName,
      List<Integer> permanentErrors,
      String cmd,
      int immediateRetries,
      int maximumRetries) {

    StageEntity stageEntity =
        assertSubmittedExecutorStageEntity(stageService, pipelineName, processId, stageName);

    assertSimpleLsfExecutorStageEntity(
        permanentErrors, cmd, immediateRetries, maximumRetries, stageEntity, lsfTestConfiguration);
  }

  public static void assertCompletedSimpleLsfExecutorStageEntity(
      TestType testType,
      StageService stageService,
      LsfTestConfiguration lsfTestConfiguration,
      String pipelineName,
      String processId,
      String stageName,
      List<Integer> permanentErrors,
      String cmd,
      int exitCode,
      int immediateRetries,
      int maximumRetries) {

    StageEntity stageEntity =
        assertCompletedExecutorStageEntity(
            stageService,
            pipelineName,
            processId,
            stageName,
            testType,
            immediateRetries,
            maximumRetries);

    assertSimpleLsfExecutorStageEntity(
        permanentErrors, cmd, immediateRetries, maximumRetries, stageEntity, lsfTestConfiguration);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
    assertThat(stageEntity.getExitCode()).isEqualTo(exitCode);
  }
}
