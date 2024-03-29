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
package pipelite.tester;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mockito.Mockito;
import pipelite.entity.StageEntity;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.AbstractSlurmExecutor;
import pipelite.executor.CmdExecutor;
import pipelite.executor.KubernetesExecutor;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.tester.pipeline.ExecutorTestExitCode;

public class TestType {
  static final int EXIT_CODE_SUCCESS = 0;
  static final int EXIT_CODE_NON_PERMANENT_ERROR = 1;
  static final int EXIT_CODE_PERMANENT_ERROR = 2;
  static final int DEFAULT_IMMEDIATE_RETRIES = 3;
  static final int DEFAULT_MAXIMUM_RETRIES = 3;

  @Value
  private static class StageMapKey {
    private final String pipelineName;
    private final String processId;
    private final String stageName;
  }

  private static ConcurrentHashMap<StageMapKey, TestType> stageTestType;
  private static ConcurrentHashMap<StageMapKey, Integer> stageExecCount;
  private static ConcurrentHashMap<StageMapKey, String> stageLastExitCode;
  private static ConcurrentHashMap<StageMapKey, String> stageNextExitCode;

  private final String description;
  private final List<Integer> exitCodes;
  private final int immediateRetries;
  private final int maximumRetries;
  private final List<String> failedAsserts = new ArrayList<>();

  public static List<TestType> init() {
    stageTestType = new ConcurrentHashMap<>();
    stageExecCount = new ConcurrentHashMap<>();
    stageLastExitCode = new ConcurrentHashMap<>();
    stageNextExitCode = new ConcurrentHashMap<>();

    List<TestType> tests = new ArrayList<>();
    tests.add(firstExecutionIsSuccessfulTest());
    tests.add(firstExecutionIsPermanentErrorTest());
    tests.add(nonPermanentErrorUntilMaximumRetriesTest());
    tests.add(nonPermanentErrorAndThenSuccessTest());
    tests.add(nonPermanentErrorAndThenPermanentErrorTest());
    return tests;
  }

  static TestType firstExecutionIsSuccessfulTest() {
    return new TestType(
        "First execution is successful",
        Arrays.asList(EXIT_CODE_SUCCESS),
        DEFAULT_IMMEDIATE_RETRIES,
        DEFAULT_MAXIMUM_RETRIES);
  }

  static TestType firstExecutionIsPermanentErrorTest() {
    return new TestType(
        "First execution is permanent error",
        Arrays.asList(EXIT_CODE_PERMANENT_ERROR),
        DEFAULT_IMMEDIATE_RETRIES,
        DEFAULT_MAXIMUM_RETRIES);
  }

  static TestType nonPermanentErrorUntilMaximumRetriesTest() {
    return new TestType(
        "Non permanent error until maximum retries",
        Collections.nCopies(DEFAULT_MAXIMUM_RETRIES + 1, EXIT_CODE_NON_PERMANENT_ERROR),
        DEFAULT_IMMEDIATE_RETRIES,
        DEFAULT_MAXIMUM_RETRIES);
  }

  static TestType nonPermanentErrorAndThenSuccessTest() {
    return new TestType(
        "Non permanent error and then success",
        Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR, EXIT_CODE_SUCCESS),
        DEFAULT_IMMEDIATE_RETRIES,
        DEFAULT_MAXIMUM_RETRIES);
  }

  static TestType nonPermanentErrorAndThenPermanentErrorTest() {
    return new TestType(
        "Non permanent error and then permanent error",
        Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR, EXIT_CODE_PERMANENT_ERROR),
        DEFAULT_IMMEDIATE_RETRIES,
        DEFAULT_MAXIMUM_RETRIES);
  }

  private TestType(
      String description, List<Integer> exitCodes, int immediateRetries, int maximumRetries) {
    this.description =
        description
            + ", ExitCodes: "
            + exitCodes.stream().map(s -> String.valueOf(s)).collect(Collectors.joining(","))
            + ", ImmediateRetries: "
            + immediateRetries
            + ", MaximumRetries: "
            + maximumRetries
            + ", Success: "
            + EXIT_CODE_SUCCESS
            + ", NonPermanentError: "
            + EXIT_CODE_NON_PERMANENT_ERROR
            + ", PermanentError: "
            + EXIT_CODE_PERMANENT_ERROR;
    this.exitCodes = exitCodes;
    this.immediateRetries = immediateRetries;
    this.maximumRetries = maximumRetries;
  }

  public String description() {
    return description;
  }

  public int immediateRetries() {
    return immediateRetries;
  }

  public int maximumRetries() {
    return maximumRetries;
  }

  static boolean noMoreRetries(int callCnt, int maximumRetries) {
    return callCnt > maximumRetries;
  }

  static int expectedStageFailedCnt(List<Integer> exitCodes, int maximumRetries) {
    int failedCnt = 0;
    int callCnt = 0;
    for (int exitCode : exitCodes) {
      if (noMoreRetries(callCnt, maximumRetries)) {
        break;
      }
      callCnt++;
      if (exitCode == EXIT_CODE_SUCCESS) {
        break;
      }
      if (exitCode == EXIT_CODE_PERMANENT_ERROR) {
        failedCnt++;
        break;
      }
      if (exitCode == EXIT_CODE_NON_PERMANENT_ERROR) {
        failedCnt++;
        // continue
      }
    }
    return failedCnt;
  }

  public int expectedStageFailedCnt() {
    return expectedStageFailedCnt(exitCodes, maximumRetries);
  }

  static int expectedStagePermanentErrorCnt(List<Integer> exitCodes, int maximumRetries) {
    int errorCnt = 0;
    int callCnt = 0;
    for (int exitCode : exitCodes) {
      if (noMoreRetries(callCnt, maximumRetries)) {
        break;
      }
      callCnt++;
      if (exitCode == EXIT_CODE_SUCCESS) {
        break;
      }
      if (exitCode == EXIT_CODE_PERMANENT_ERROR) {
        errorCnt++;
        break;
      }
    }
    return errorCnt;
  }

  public int expectedStagePermanentErrorCnt() {
    return expectedStagePermanentErrorCnt(exitCodes, maximumRetries);
  }

  public int expectedStageNonPermanentErrorCnt() {
    int errorCnt = 0;
    int callCnt = 0;
    for (int exitCode : exitCodes) {
      if (noMoreRetries(callCnt, maximumRetries)) {
        break;
      }
      callCnt++;
      if (exitCode == EXIT_CODE_SUCCESS) {
        break;
      }
      if (exitCode == EXIT_CODE_PERMANENT_ERROR) {
        break;
      }
      if (exitCode == EXIT_CODE_NON_PERMANENT_ERROR) {
        errorCnt++;
        // continue
      }
    }
    return errorCnt;
  }

  static int expectedStageExecutionCnt(List<Integer> exitCodes, int maximumRetries) {
    int callCnt = 0;
    for (int exitCode : exitCodes) {
      if (noMoreRetries(callCnt, maximumRetries)) {
        break;
      }
      callCnt++;
      if (exitCode == EXIT_CODE_SUCCESS) {
        break;
      }
      if (exitCode == EXIT_CODE_PERMANENT_ERROR) {
        break;
      }
    }
    return callCnt;
  }

  public int expectedStageExecutionCnt() {
    return expectedStageExecutionCnt(exitCodes, maximumRetries);
  }

  static int expectedStageSuccessCnt(List<Integer> exitCodes, int maximumRetries) {
    int successCnt = 0;
    int callCnt = 0;
    for (int exitCode : exitCodes) {
      if (noMoreRetries(callCnt, maximumRetries)) {
        break;
      }
      callCnt++;
      if (exitCode == EXIT_CODE_SUCCESS) {
        successCnt++;
        break;
      }
      // continue
    }
    return successCnt;
  }

  public int expectedStageSuccessCnt() {
    return expectedStageSuccessCnt(exitCodes, maximumRetries);
  }

  public int expectedProcessFailedCnt() {
    return expectedStageSuccessCnt() > 0 ? 0 : 1;
  }

  public int expectedProcessCompletedCnt() {
    return expectedStageSuccessCnt() > 0 ? 1 : 0;
  }

  public List<String> failedAsserts() {
    return failedAsserts;
  }

  String exitCode(int execCount) {
    if (exitCodes.size() >= execCount + 1) {
      return String.valueOf(exitCodes.get(execCount));
    }
    return "";
  }

  public int nextExitCode(String pipelineName, String processId, String stageName) {
    String nextExitCode = stageNextExitCode.get(registeredKey(pipelineName, processId, stageName));
    if (nextExitCode == null || nextExitCode.equals("")) {
      throw new RuntimeException("Stage has no next exit code");
    }
    return Integer.valueOf(nextExitCode);
  }

  public int lastExitCode(String pipelineName, String processId, String stageName) {
    String lastExitCode = stageLastExitCode.get(registeredKey(pipelineName, processId, stageName));
    if (lastExitCode == null || lastExitCode.equals("")) {
      throw new RuntimeException("Stage has no last exit code");
    }
    return Integer.valueOf(lastExitCode);
  }

  public List<Integer> permanentErrors() {
    return Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR);
  }

  private static StageMapKey key(String pipelineName, String processId, String stageName) {
    return new StageMapKey(pipelineName, processId, stageName);
  }

  private static StageMapKey registeredKey(
      String pipelineName, String processId, String stageName) {
    StageMapKey key = key(pipelineName, processId, stageName);
    // Check that stage has been registered.
    testType(key);
    return key;
  }

  private static StageMapKey registeredKey(Stage stage) {
    return registeredKey(
        stage.getStageEntity().getPipelineName(),
        stage.getStageEntity().getProcessId(),
        stage.getStageName());
  }

  public void register(String pipelineName, String processId, String stageName) {
    StageMapKey key = key(pipelineName, processId, stageName);
    if (stageTestType.contains(key)) {
      throw new RuntimeException("Stage has already been registered");
    }
    stageTestType.put(key, this);
    stageNextExitCode.put(key, exitCode(0));
  }

  private interface Action {
    void apply();
  }

  private static <T> void handleErrors(AtomicReference<TestType> testTypeRef, Action action) {
    try {
      action.apply();
    } catch (Exception ex) {
      TestType testType = testTypeRef.get();
      if (testType != null) {
        testType.failedAsserts.add(
            "Unexpected exception from TestType spyStageService: "
                + ExceptionUtils.getStackTrace(ex));
      } else {
        throw new RuntimeException(
            "Unexpected exception from TestType spyStageService: " + ex.getMessage(), ex);
      }
    }
  }

  public static void spyStageService(StageService stageServiceSpy) {
    if (!Mockito.mockingDetails(stageServiceSpy).isSpy()) {
      throw new RuntimeException("StageService must be a spy");
    }
    // Last and next exit code
    doAnswer(
            invocation -> {
              StageEntity stageEntity = (StageEntity) invocation.callRealMethod();
              AtomicReference<TestType> testTypeRef = new AtomicReference<>();
              handleErrors(
                  testTypeRef,
                  () -> {
                    Stage stage = invocation.getArgument(0);
                    StageExecutorResult result = invocation.getArgument(1);

                    StageMapKey key = registeredKey(stage);
                    TestType testType = testType(key);
                    testTypeRef.set(testType);

                    String lastExitCode = result.exitCode();
                    if (lastExitCode != null) {
                      setLastExitCode(lastExitCode, key);
                    }
                    if (result.isTimeoutError()) {
                      testType.failedAsserts.add("Stage timeout error");
                    }
                    if (lastExitCode == null) {
                      testType.failedAsserts.add("Stage has no exit code");
                    } else if (!lastExitCode.equals(stageNextExitCode.get(key))) {
                      testType.failedAsserts.add(
                          "Unexpected stage exit code "
                              + lastExitCode
                              + " and not "
                              + stageNextExitCode.get(key));
                    }
                    setNextExitCode(testType, stage, key);
                  });
              return stageEntity;
            })
        .when(stageServiceSpy)
        .endExecution(any(), any());

    // Stage deserialization
    doAnswer(
            invocation -> {
              Stage stage = invocation.getArgument(0);
              StageEntity savedStageEntity = (StageEntity) invocation.callRealMethod();
              AtomicReference<TestType> testTypeRef = new AtomicReference<>();
              handleErrors(
                  testTypeRef,
                  () -> {
                    StageMapKey key = registeredKey(stage);
                    TestType testType = testType(key);
                    testTypeRef.set(testType);

                    if (!stage.getStageEntity().equals(savedStageEntity)) {
                      testType.failedAsserts.add(
                          "Stage entity "
                              + stage.getStageEntity().toString()
                              + " is different from saved stage entity: "
                              + savedStageEntity.toString());
                    }
                  });
              return null;
            })
        .when(stageServiceSpy)
        .saveStage(any());
  }

  static void setLastExitCode(
      String lastExitCode, String pipelineName, String processId, String stageName) {
    setLastExitCode(lastExitCode, key(pipelineName, processId, stageName));
  }

  static void setLastExitCode(String lastExitCode, StageMapKey key) {
    stageLastExitCode.put(key, lastExitCode);
  }

  static void setNextExitCode(
      TestType testType, Stage stage, String pipelineName, String processId, String stageName) {
    setNextExitCode(testType, stage, key(pipelineName, processId, stageName));
  }

  static void setNextExitCode(TestType testType, Stage stage, StageMapKey key) {
    // Execution count.
    Integer execCount = stageExecCount.get(key);
    if (execCount == null) {
      execCount = 0;
    }
    ++execCount;
    stageExecCount.put(key, execCount);
    // Next exit code.
    String nextExitCode = testType.exitCode(execCount);
    stageNextExitCode.put(key, nextExitCode);

    String pipelineName = key.getPipelineName();
    String processId = key.getProcessId();
    String stageName = key.getStageName();

    if (!nextExitCode.equals("")) {
      if (stage.getExecutor() instanceof KubernetesExecutor) {
        KubernetesExecutor executor = (KubernetesExecutor) stage.getExecutor();
        executor.setImageArgs(
            ExecutorTestExitCode.cmdAsArray(
                testType.nextExitCode(pipelineName, processId, stageName)));
      } else if (stage.getExecutor() instanceof AbstractLsfExecutor<?>) {
        AbstractLsfExecutor<?> executor = (AbstractLsfExecutor<?>) stage.getExecutor();
        executor.setCmd(
            ExecutorTestExitCode.cmdAsString(
                testType.nextExitCode(pipelineName, processId, stageName)));
      } else if (stage.getExecutor() instanceof AbstractSlurmExecutor<?>) {
        AbstractSlurmExecutor<?> executor = (AbstractSlurmExecutor<?>) stage.getExecutor();
        executor.setCmd(
            ExecutorTestExitCode.cmdAsString(
                testType.nextExitCode(pipelineName, processId, stageName)));
      } else if (stage.getExecutor() instanceof CmdExecutor<?>) {
        CmdExecutor<?> executor = (CmdExecutor<?>) stage.getExecutor();
        executor.setCmd(
            ExecutorTestExitCode.cmdAsString(
                testType.nextExitCode(pipelineName, processId, stageName)));
      } else {
        testType.failedAsserts.add(
            "Unexpected executor type when changing executor exit code: "
                + stage.getExecutor().getClass().getSimpleName());
      }
    }
  }

  private static TestType testType(StageMapKey key) {
    TestType testType = stageTestType.get(key);
    if (testType == null) {
      throw new RuntimeException("Stage has not been registered");
    }
    return testType;
  }
}
