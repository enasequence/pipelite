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
import pipelite.executor.AbstractAsyncExecutor;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.CmdExecutor;
import pipelite.executor.KubernetesExecutor;
import pipelite.executor.describe.cache.DescribeJobsCache;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.executor.StageExecutorSerializer;

public class TestType {
  static final int EXIT_CODE_SUCCESS = 0;
  static final int EXIT_CODE_NON_PERMANENT_ERROR = 1;
  static final int EXIT_CODE_PERMANENT_ERROR = 2;
  static final int DEFAULT_IMMEDIATE_RETRIES = 3;
  static final int DEFAULT_MAXIMUM_RETRIES = 3;

  private final String description;
  private final List<Integer> exitCodes;
  private final int immediateRetries;
  private final int maximumRetries;
  private final boolean deserializeActiveStageEntity;

  private List<String> failedAsserts = new ArrayList<>();

  public static final List<TestType> tests = new ArrayList<>();
  static final TestType firstExecutionIsSuccessful;
  static final TestType firstExecutionIsPermanentError;
  static final TestType nonPermanentErrorUntilMaximumRetries;
  static final TestType nonPermanentErrorAndThenSuccess;
  static final TestType nonPermanentErrorAndThenPermanentError;

  static {
    firstExecutionIsSuccessful =
        new TestType(
            "First execution is successful",
            Arrays.asList(EXIT_CODE_SUCCESS),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            false);
    tests.add(firstExecutionIsSuccessful);

    firstExecutionIsPermanentError =
        new TestType(
            "First execution is permanent error",
            Arrays.asList(EXIT_CODE_PERMANENT_ERROR),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            false);
    tests.add(firstExecutionIsPermanentError);

    nonPermanentErrorUntilMaximumRetries =
        new TestType(
            "Non permanent error until maximum retries",
            Collections.nCopies(DEFAULT_MAXIMUM_RETRIES + 1, EXIT_CODE_NON_PERMANENT_ERROR),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            false);
    tests.add(nonPermanentErrorUntilMaximumRetries);

    nonPermanentErrorAndThenSuccess =
        new TestType(
            "Non permanent error and then success",
            Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR, EXIT_CODE_SUCCESS),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            false);
    tests.add(nonPermanentErrorAndThenSuccess);

    TestType nonPermanentErrorAndThenSuccessWithDeserializeActiveStageEntity =
        new TestType(
            "Non permanent error and then success with deserialize active stage entity",
            Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR, EXIT_CODE_SUCCESS),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            true);
    tests.add(nonPermanentErrorAndThenSuccessWithDeserializeActiveStageEntity);

    nonPermanentErrorAndThenPermanentError =
        new TestType(
            "Non permanent error and then permanent error",
            Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR, EXIT_CODE_PERMANENT_ERROR),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            false);
    tests.add(nonPermanentErrorAndThenPermanentError);

    TestType nonPermanentErrorAndThenPermanentErrorWithDeserializeActiveStageEntity =
        new TestType(
            "Non permanent error and then permanent error with deserialize active stage entity",
            Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR, EXIT_CODE_PERMANENT_ERROR),
            DEFAULT_IMMEDIATE_RETRIES,
            DEFAULT_MAXIMUM_RETRIES,
            true);
    tests.add(nonPermanentErrorAndThenPermanentErrorWithDeserializeActiveStageEntity);
  }

  @Value
  private static class StageMapKey {
    private final String pipelineName;
    private final String processId;
    private final String stageName;
  }

  private static ConcurrentHashMap<StageMapKey, TestType> stageTestType = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<StageMapKey, Integer> stageExecCount = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<StageMapKey, String> stageLastExitCode =
      new ConcurrentHashMap<>();
  private static ConcurrentHashMap<StageMapKey, String> stageNextExitCode =
      new ConcurrentHashMap<>();

  TestType(
      String description,
      List<Integer> exitCodes,
      int immediateRetries,
      int maximumRetries,
      boolean deserializeActiveStageEntity) {
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
            + EXIT_CODE_PERMANENT_ERROR
            + ", DeserializeStageEntity: "
            + deserializeActiveStageEntity;
    this.exitCodes = exitCodes;
    this.immediateRetries = immediateRetries;
    this.maximumRetries = maximumRetries;
    this.deserializeActiveStageEntity = deserializeActiveStageEntity;
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

  public String nextExitCode(String pipelineName, String processId, String stageName) {
    String nextExitCode = stageNextExitCode.get(registeredKey(pipelineName, processId, stageName));
    if (nextExitCode == null || nextExitCode.equals("")) {
      throw new RuntimeException("Stage has no next exit code");
    }
    return nextExitCode;
  }

  public String lastExitCode(String pipelineName, String processId, String stageName) {
    String lastExitCode = stageLastExitCode.get(registeredKey(pipelineName, processId, stageName));
    if (lastExitCode == null || lastExitCode.equals("")) {
      throw new RuntimeException("Stage has no last exit code");
    }
    return lastExitCode;
  }

  public String nextCmd(String pipelineName, String processId, String stageName) {
    return "bash -c 'exit " + nextExitCode(pipelineName, processId, stageName) + "'";
  }

  public String lastCmd(String pipelineName, String processId, String stageName) {
    return "bash -c 'exit " + lastExitCode(pipelineName, processId, stageName) + "'";
  }

  public String image() {
    return "debian:10.11";
  }

  public List<String> nextImageArgs(String pipelineName, String processId, String stageName) {
    return Arrays.asList("bash", "-c", "exit " + nextExitCode(pipelineName, processId, stageName));
  }

  public List<String> lastImageArgs(String pipelineName, String processId, String stageName) {
    return Arrays.asList("bash", "-c", "exit " + lastExitCode(pipelineName, processId, stageName));
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

                    String lastExitCode =
                        result.getAttribute(StageExecutorResultAttribute.EXIT_CODE);
                    if (lastExitCode == null) {
                      testType.failedAsserts.add("Stage has no exit code");
                    }
                    else if (!lastExitCode.equals(stageNextExitCode.get(key))) {
                      testType.failedAsserts.add(
                          "Unexpected stage exit code "
                              + lastExitCode
                              + " and not "
                              + stageNextExitCode.get(key));
                    }
                    setLastExitCode(lastExitCode, key);
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

                    if (testType.deserializeActiveStageEntity
                        && stage.getStageEntity().getStageState() == StageState.ACTIVE) {
                      // Use saved stage entity.
                      stage.setStageEntity(savedStageEntity);
                      // Deserialize saved stage entity.
                      // Preserve describe jobs cache.
                      DescribeJobsCache describeJobsCache = null;
                      if (stage.getExecutor() instanceof AbstractAsyncExecutor) {
                        describeJobsCache =
                            ((AbstractAsyncExecutor) stage.getExecutor()).getDescribeJobsCache();
                      }
                      if (!StageExecutorSerializer.deserializeExecution(stage)) {
                        testType.failedAsserts.add("Could not deserialize stage entity");
                      }
                      if (describeJobsCache != null) {
                        ((AbstractAsyncExecutor) stage.getExecutor())
                            .setDescribeJobsCache(describeJobsCache);
                      }
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
        executor.setImageArgs(testType.nextImageArgs(pipelineName, processId, stageName));
      } else if (stage.getExecutor() instanceof AbstractLsfExecutor<?>) {
        AbstractLsfExecutor<?> executor = (AbstractLsfExecutor<?>) stage.getExecutor();
        executor.setCmd(testType.nextCmd(pipelineName, processId, stageName));
      } else if (stage.getExecutor() instanceof CmdExecutor<?>) {
        CmdExecutor<?> executor = (CmdExecutor<?>) stage.getExecutor();
        executor.setCmd(testType.nextCmd(pipelineName, processId, stageName));
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
