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
package pipelite.tester.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.entity.StageEntity;
import pipelite.service.StageService;
import pipelite.stage.StageState;
import pipelite.stage.executor.ErrorType;
import pipelite.tester.TestType;
import pipelite.tester.TestTypeConfiguration;

public class StageEntityAsserter {
  private StageEntityAsserter() {}

  private static StageEntity assertSubmittedStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

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

  private static StageEntity assertCompletedStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    TestType testType = testConfiguration.testType();
    int immediateRetries = testConfiguration.immediateRetries();
    int maximumRetries = testConfiguration.maximumRetries();
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
    int expectedExecutionCountBasedOnSuccessAfterError =
        testType == TestType.SUCCESS_AFTER_ONE_NON_PERMANENT_ERROR ? 2 : 1;

    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getStageState()).isEqualTo(expectedStageState);
    assertThat(stageEntity.getErrorType()).isEqualTo(expectedErrorType);
    assertThat(stageEntity.getExecutionCount())
        .isIn(
            expectedExecutionCountBasedOnImmediateRetries,
            expectedExecutionCountBasedOnMaximumRetries,
            expectedExecutionCountBasedOnSuccessAfterError);
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isAfterOrEqualTo(stageEntity.getStartTime());
    return stageEntity;
  }

  public static void assertTestExecutorStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    StageEntity stageEntity =
        assertCompletedStageEntity(
            stageService, testConfiguration, pipelineName, processId, stageName);

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.TestExecutor");

    assertThat(stageEntity.getExecutorParams())
        .contains("\"maximumRetries\" : " + testConfiguration.maximumRetries());
    assertThat(stageEntity.getExecutorParams())
        .contains("\"immediateRetries\" : " + testConfiguration.immediateRetries());
  }

  private static void assertSimpleLsfStageEntity(
      TestTypeConfiguration testConfiguration,
      LsfTestConfiguration lsfTestConfiguration,
      StageEntity stageEntity) {
    String cmd =
        testConfiguration.lastCmd(
            stageEntity.getPipelineName(), stageEntity.getProcessId(), stageEntity.getStageName());
    List<Integer> permanentErrors =
        testConfiguration.lastPermanentErrors(
            stageEntity.getPipelineName(), stageEntity.getProcessId(), stageEntity.getStageName());

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");

    assertThat(stageEntity.getExecutorData()).contains("\"state\" : \"POLL\"");
    assertThat(stageEntity.getExecutorData()).contains("\"jobId\" : \"");
    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + cmd + "\"");
    assertThat(stageEntity.getExecutorData()).contains("  \"outFile\" : \"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + testConfiguration.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testConfiguration.immediateRetries()
                + ",\n"
                + (permanentErrors != null && !permanentErrors.isEmpty()
                    ? "  \"permanentErrors\" : [ "
                        + String.join(
                            ", ",
                            permanentErrors.stream()
                                .map(i -> i.toString())
                                .collect(Collectors.toList()))
                        + " ],\n"
                    : "")
                + "  \"saveLog\" : true,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000,\n"
                + "  \"host\" : \""
                + lsfTestConfiguration.getHost()
                + "\",\n"
                + "  \"workDir\" : \""
                + lsfTestConfiguration.getWorkDir()
                + "\"\n"
                + "}");
  }

  private static void assertKubernetesStageEntity(
      TestTypeConfiguration testConfiguration,
      KubernetesTestConfiguration kubernetesTestConfiguration,
      StageEntity stageEntity) {
    List<Integer> permanentErrors =
        testConfiguration.lastPermanentErrors(
            stageEntity.getPipelineName(), stageEntity.getProcessId(), stageEntity.getStageName());
    String namespace = kubernetesTestConfiguration.getNamespace();

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.KubernetesExecutor");

    assertThat(stageEntity.getExecutorData()).contains("\"state\" : \"POLL\"");
    assertThat(stageEntity.getExecutorData()).contains("\"jobId\" : \"");
    assertThat(stageEntity.getExecutorData())
        .contains("\"image\" : \"" + testConfiguration.image() + "\"");
    assertThat(stageEntity.getExecutorData()).contains("\"imageArgs\" : [");
    assertThat(stageEntity.getExecutorData()).contains("\"namespace\" : \"" + namespace + "\"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + testConfiguration.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testConfiguration.immediateRetries()
                + ",\n"
                + (permanentErrors != null && !permanentErrors.isEmpty()
                    ? "  \"permanentErrors\" : [ "
                        + String.join(
                            ", ",
                            permanentErrors.stream()
                                .map(i -> i.toString())
                                .collect(Collectors.toList()))
                        + " ],\n"
                    : "")
                + "  \"saveLog\" : true,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000,\n"
                + "  \"namespace\" : \""
                + namespace
                + "\"\n"
                + "}");
  }

  private static void assertCmdStageEntity(
      TestTypeConfiguration testConfiguration, StageEntity stageEntity) {
    String cmd =
        testConfiguration.lastCmd(
            stageEntity.getPipelineName(), stageEntity.getProcessId(), stageEntity.getStageName());
    List<Integer> permanentErrors =
        testConfiguration.lastPermanentErrors(
            stageEntity.getPipelineName(), stageEntity.getProcessId(), stageEntity.getStageName());

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.CmdExecutor");

    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + cmd + "\"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 604800000,\n"
                + "  \"maximumRetries\" : "
                + testConfiguration.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testConfiguration.immediateRetries()
                + ",\n"
                + (permanentErrors != null && !permanentErrors.isEmpty()
                    ? "  \"permanentErrors\" : [ "
                        + String.join(
                            ", ",
                            permanentErrors.stream()
                                .map(i -> i.toString())
                                .collect(Collectors.toList()))
                        + " ],\n"
                    : "")
                + "  \"saveLog\" : true,\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"logTimeout\" : 10000\n"
                + "}");
  }

  public static void assertSubmittedSimpleLsfStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      LsfTestConfiguration lsfTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    StageEntity stageEntity =
        assertSubmittedStageEntity(
            stageService, testConfiguration, pipelineName, processId, stageName);

    assertSimpleLsfStageEntity(testConfiguration, lsfTestConfiguration, stageEntity);
  }

  public static void assertSubmittedKubernetesStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      KubernetesTestConfiguration kubernetesTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    StageEntity stageEntity =
        assertSubmittedStageEntity(
            stageService, testConfiguration, pipelineName, processId, stageName);

    assertKubernetesStageEntity(testConfiguration, kubernetesTestConfiguration, stageEntity);
  }

  public static void assertCompletedSimpleLsfStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      LsfTestConfiguration lsfTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = testConfiguration.lastExitCode(pipelineName, processId, stageName);

    StageEntity stageEntity =
        assertCompletedStageEntity(
            stageService, testConfiguration, pipelineName, processId, stageName);

    assertSimpleLsfStageEntity(testConfiguration, lsfTestConfiguration, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
    assertThat(String.valueOf(stageEntity.getExitCode())).isEqualTo(exitCode);
  }

  public static void assertCompletedKubernetesStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      KubernetesTestConfiguration kubernetesTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = testConfiguration.lastExitCode(pipelineName, processId, stageName);

    StageEntity stageEntity =
        assertCompletedStageEntity(
            stageService, testConfiguration, pipelineName, processId, stageName);

    assertKubernetesStageEntity(testConfiguration, kubernetesTestConfiguration, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
    assertThat(String.valueOf(stageEntity.getExitCode())).isEqualTo(exitCode);
  }

  public static void assertCompletedCmdStageEntity(
      StageService stageService,
      TestTypeConfiguration testConfiguration,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = testConfiguration.lastExitCode(pipelineName, processId, stageName);

    StageEntity stageEntity =
        assertCompletedStageEntity(
            stageService, testConfiguration, pipelineName, processId, stageName);

    assertCmdStageEntity(testConfiguration, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getExitCode().toString()).isEqualTo(exitCode);
  }
}
