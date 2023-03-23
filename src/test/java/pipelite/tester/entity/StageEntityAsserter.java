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
package pipelite.tester.entity;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.configuration.properties.SlurmTestConfiguration;
import pipelite.entity.StageEntity;
import pipelite.entity.field.ErrorType;
import pipelite.entity.field.StageState;
import pipelite.service.StageService;
import pipelite.tester.TestType;
import pipelite.tester.pipeline.ExecutorTestExitCode;

public class StageEntityAsserter {
  private StageEntityAsserter() {}

  private static StageEntity assertSubmittedStageEntity(
      StageService stageService,
      TestType testType,
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
      TestType testType,
      String pipelineName,
      String processId,
      String stageName) {

    StageState expectedStageState;
    ErrorType expectedErrorType = null;
    if (testType.expectedStagePermanentErrorCnt() > 0) {
      expectedStageState = StageState.ERROR;
      expectedErrorType = ErrorType.PERMANENT_ERROR;
    } else if (testType.expectedStageSuccessCnt() > 0) {
      expectedStageState = StageState.SUCCESS;
    } else {
      expectedStageState = StageState.ERROR;
      expectedErrorType = ErrorType.EXECUTION_ERROR;
    }

    StageEntity stageEntity = stageService.getSavedStage(pipelineName, processId, stageName).get();
    assertThat(stageEntity.getPipelineName()).isEqualTo(pipelineName);
    assertThat(stageEntity.getProcessId()).isEqualTo(processId);
    assertThat(stageEntity.getStageName()).isEqualTo(stageName);
    assertThat(stageEntity.getStageState()).isEqualTo(expectedStageState);
    assertThat(stageEntity.getErrorType()).isEqualTo(expectedErrorType);
    assertThat(stageEntity.getExecutionCount()).isEqualTo(testType.expectedStageExecutionCnt());
    assertThat(stageEntity.getStartTime()).isNotNull();
    assertThat(stageEntity.getEndTime()).isAfterOrEqualTo(stageEntity.getStartTime());
    return stageEntity;
  }

  private static void assertSimpleLsfStageEntity(
      TestType testType, LsfTestConfiguration lsfTestConfiguration, StageEntity stageEntity) {
    String cmd =
        ExecutorTestExitCode.cmdAsString(
            testType.lastExitCode(
                stageEntity.getPipelineName(),
                stageEntity.getProcessId(),
                stageEntity.getStageName()));
    List<Integer> permanentErrors = testType.permanentErrors();

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleLsfExecutor");

    assertThat(stageEntity.getExecutorData()).contains("\"jobId\" : \"");
    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + cmd + "\"");
    assertThat(stageEntity.getExecutorData()).contains("  \"outFile\" : \"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + testType.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testType.immediateRetries()
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
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"host\" : \""
                + lsfTestConfiguration.getHost()
                + "\",\n"
                + "  \"user\" : \""
                + lsfTestConfiguration.getUser()
                + "\",\n"
                + "  \"logDir\" : \""
                + lsfTestConfiguration.getLogDir()
                + "\",\n"
                + "  \"logTimeout\" : 10000,\n"
                + "  \"queue\" : \""
                + lsfTestConfiguration.getQueue()
                + "\"\n"
                + "}");
  }

  private static void assertSimpleSlurmStageEntity(
      TestType testType, SlurmTestConfiguration slurmTestConfiguration, StageEntity stageEntity) {
    String cmd =
        ExecutorTestExitCode.cmdAsString(
            testType.lastExitCode(
                stageEntity.getPipelineName(),
                stageEntity.getProcessId(),
                stageEntity.getStageName()));
    List<Integer> permanentErrors = testType.permanentErrors();

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.SimpleSlurmExecutor");

    assertThat(stageEntity.getExecutorData()).contains("\"jobId\" : \"");
    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + cmd + "\"");
    assertThat(stageEntity.getExecutorData()).contains("  \"outFile\" : \"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + testType.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testType.immediateRetries()
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
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"host\" : \""
                + slurmTestConfiguration.getHost()
                + "\",\n"
                + "  \"user\" : \""
                + slurmTestConfiguration.getUser()
                + "\",\n"
                + "  \"logDir\" : \""
                + slurmTestConfiguration.getLogDir()
                + "\",\n"
                + "  \"logTimeout\" : 10000,\n"
                + "  \"queue\" : \""
                + slurmTestConfiguration.getQueue()
                + "\",\n"
                + "  \"cpu\" : 1,\n"
                + "  \"memory\" : 1\n"
                + "}");
  }

  private static void assertKubernetesStageEntity(
      TestType testType,
      KubernetesTestConfiguration kubernetesTestConfiguration,
      StageEntity stageEntity) {
    List<Integer> permanentErrors = testType.permanentErrors();
    String namespace = kubernetesTestConfiguration.getNamespace();

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.KubernetesExecutor");

    assertThat(stageEntity.getExecutorData()).contains("\"jobId\" : \"");
    assertThat(stageEntity.getExecutorData())
        .contains("\"image\" : \"" + ExecutorTestExitCode.IMAGE + "\"");
    assertThat(stageEntity.getExecutorData()).contains("\"imageArgs\" : [");
    assertThat(stageEntity.getExecutorData()).contains("\"namespace\" : \"" + namespace + "\"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 180000,\n"
                + "  \"maximumRetries\" : "
                + testType.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testType.immediateRetries()
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
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000,\n"
                + "  \"namespace\" : \""
                + namespace
                + "\"\n"
                + "}");
  }

  private static void assertCmdStageEntity(TestType testType, StageEntity stageEntity) {
    String cmd =
        ExecutorTestExitCode.cmdAsString(
            testType.lastExitCode(
                stageEntity.getPipelineName(),
                stageEntity.getProcessId(),
                stageEntity.getStageName()));
    List<Integer> permanentErrors = testType.permanentErrors();

    assertThat(stageEntity.getExecutorName()).isEqualTo("pipelite.executor.CmdExecutor");

    assertThat(stageEntity.getExecutorData()).contains("  \"cmd\" : \"" + cmd + "\"");

    assertThat(stageEntity.getExecutorParams())
        .isEqualTo(
            "{\n"
                + "  \"timeout\" : 604800000,\n"
                + "  \"maximumRetries\" : "
                + testType.maximumRetries()
                + ",\n"
                + "  \"immediateRetries\" : "
                + testType.immediateRetries()
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
                + "  \"logSave\" : \"ERROR\",\n"
                + "  \"logLines\" : 1000\n"
                + "}");
  }

  public static void assertSubmittedSimpleLsfStageEntity(
      StageService stageService,
      TestType testType,
      LsfTestConfiguration lsfTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    StageEntity stageEntity =
        assertSubmittedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertSimpleLsfStageEntity(testType, lsfTestConfiguration, stageEntity);
  }

  public static void assertSubmittedSimpleSlurmStageEntity(
      StageService stageService,
      TestType testType,
      SlurmTestConfiguration slurmTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    StageEntity stageEntity =
        assertSubmittedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertSimpleSlurmStageEntity(testType, slurmTestConfiguration, stageEntity);
  }

  public static void assertSubmittedKubernetesStageEntity(
      StageService stageService,
      TestType testType,
      KubernetesTestConfiguration kubernetesTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {

    StageEntity stageEntity =
        assertSubmittedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertKubernetesStageEntity(testType, kubernetesTestConfiguration, stageEntity);
  }

  public static void assertCompletedSimpleLsfStageEntity(
      StageService stageService,
      TestType testType,
      LsfTestConfiguration lsfTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = String.valueOf(testType.lastExitCode(pipelineName, processId, stageName));

    StageEntity stageEntity =
        assertCompletedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertSimpleLsfStageEntity(testType, lsfTestConfiguration, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
    assertThat(String.valueOf(stageEntity.getExitCode())).isEqualTo(exitCode);
  }

  public static void assertCompletedSimpleSlurmStageEntity(
      StageService stageService,
      TestType testType,
      SlurmTestConfiguration slurmTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = String.valueOf(testType.lastExitCode(pipelineName, processId, stageName));

    StageEntity stageEntity =
        assertCompletedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertSimpleSlurmStageEntity(testType, slurmTestConfiguration, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
    assertThat(String.valueOf(stageEntity.getExitCode())).isEqualTo(exitCode);
  }

  public static void assertCompletedKubernetesStageEntity(
      StageService stageService,
      TestType testType,
      KubernetesTestConfiguration kubernetesTestConfiguration,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = String.valueOf(testType.lastExitCode(pipelineName, processId, stageName));

    StageEntity stageEntity =
        assertCompletedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertKubernetesStageEntity(testType, kubernetesTestConfiguration, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getResultParams()).contains("\"job id\" :");
    assertThat(String.valueOf(stageEntity.getExitCode())).isEqualTo(exitCode);
  }

  public static void assertCompletedCmdStageEntity(
      StageService stageService,
      TestType testType,
      String pipelineName,
      String processId,
      String stageName) {
    String exitCode = String.valueOf(testType.lastExitCode(pipelineName, processId, stageName));

    StageEntity stageEntity =
        assertCompletedStageEntity(stageService, testType, pipelineName, processId, stageName);

    assertCmdStageEntity(testType, stageEntity);

    assertThat(stageEntity.getResultParams()).contains("\"exit code\" : \"" + exitCode + "\"");
    assertThat(stageEntity.getExitCode().toString()).isEqualTo(exitCode);
  }
}
