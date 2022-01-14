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
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.flogger.Flogger;
import org.mockito.Mockito;
import pipelite.entity.StageEntity;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.CmdExecutor;
import pipelite.executor.KubernetesExecutor;
import pipelite.executor.TestExecutor;
import pipelite.service.StageService;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

/** Requires stageService to be spied. @SpyBean private StageService stageService; */
@Flogger
@Value
@Accessors(fluent = true)
public class TestTypeConfiguration {
  static final int EXIT_CODE_SUCCESS = 0;
  static final int EXIT_CODE_NON_PERMANENT_ERROR = 1;
  static final int EXIT_CODE_PERMANENT_ERROR = 2;

  private @NonNull TestType testType;
  private int immediateRetries;
  private int maximumRetries;
  private List<String> failedAsserts = new ArrayList<>();

  public TestTypeConfiguration(
      @NonNull TestType testType, int immediateRetries, int maximumRetries) {
    this.testType = testType;
    this.immediateRetries = immediateRetries;
    this.maximumRetries = maximumRetries;
  }

  public String nextExitCode(String pipelineName, String processId, String stageName) {
    String nextExitCode = stageNextExitCode.get(registeredKey(pipelineName, processId, stageName));
    if (nextExitCode.equals("")) {
      throw new RuntimeException("Stage has no next exit code");
    }
    return nextExitCode;
  }

  public String lastExitCode(String pipelineName, String processId, String stageName) {
    String lastExitCode = stageLastExitCode.get(registeredKey(pipelineName, processId, stageName));
    if (lastExitCode.equals("")) {
      throw new RuntimeException("Stage has no last exit code");
    }
    return lastExitCode;
  }

  public String nextCmd(String pipelineName, String processId, String stageName) {
    return "bash -c 'exit '" + nextExitCode(pipelineName, processId, stageName);
  }

  public String lastCmd(String pipelineName, String processId, String stageName) {
    return "bash -c 'exit '" + lastExitCode(pipelineName, processId, stageName);
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

  public List<Integer> nextPermanentErrors(
      String pipelineName, String processId, String stageName) {
    if (nextExitCode(pipelineName, processId, stageName)
        .equals(String.valueOf(EXIT_CODE_PERMANENT_ERROR))) {
      return Arrays.asList(EXIT_CODE_PERMANENT_ERROR);
    }
    return Collections.emptyList();
  }

  public List<Integer> lastPermanentErrors(
      String pipelineName, String processId, String stageName) {
    if (lastExitCode(pipelineName, processId, stageName)
        .equals(String.valueOf(EXIT_CODE_PERMANENT_ERROR))) {
      return Arrays.asList(EXIT_CODE_PERMANENT_ERROR);
    }
    return Collections.emptyList();
  }

  @Value
  private static class StageMapKey {
    private final String pipelineName;
    private final String processId;
    private final String stageName;
  }

  private static StageMapKey key(String pipelineName, String processId, String stageName) {
    return new StageMapKey(pipelineName, processId, stageName);
  }

  private static StageMapKey registeredKey(
      String pipelineName, String processId, String stageName) {
    StageMapKey key = key(pipelineName, processId, stageName);
    // Check that stage has been registered.
    testConfiguration(key);
    return key;
  }

  private static ConcurrentHashMap<StageMapKey, TestTypeConfiguration> stageTestConfiguration =
      new ConcurrentHashMap<>();
  private static ConcurrentHashMap<StageMapKey, Integer> stageExecCount = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<StageMapKey, String> stageLastExitCode =
      new ConcurrentHashMap<>();
  private static ConcurrentHashMap<StageMapKey, String> stageNextExitCode =
      new ConcurrentHashMap<>();

  public void register(String pipelineName, String processId, String stageName) {
    StageMapKey key = key(pipelineName, processId, stageName);
    if (stageTestConfiguration.contains(key)) {
      throw new RuntimeException("Stage has already been registered");
    }
    stageTestConfiguration.put(key, this);
    stageNextExitCode.put(key, testType.exitCode(0));
  }

  public static void init(StageService stageServiceSpy) {
    if (!Mockito.mockingDetails(stageServiceSpy).isSpy()) {
      throw new RuntimeException("StageService must be a spy");
    }
    // Intercept stage execution end.
    doAnswer(
            invocation -> {
              StageEntity stageEntity = (StageEntity) invocation.callRealMethod();
              // Do not throw any additional exception from the spied method.
              try {
                Stage stage = invocation.getArgument(0);
                StageExecutorResult result = invocation.getArgument(1);
                String pipelineName = stage.getStageEntity().getPipelineName();
                String processId = stage.getStageEntity().getProcessId();
                String stageName = stage.getStageName();

                StageMapKey key = registeredKey(pipelineName, processId, stageName);
                TestTypeConfiguration testConfiguration = testConfiguration(key);

                TestType testType = testConfiguration.testType;

                // Last exit code.
                String lastExitCode = result.getAttribute(StageExecutorResultAttribute.EXIT_CODE);
                if (lastExitCode == null) {
                  testConfiguration.failedAsserts.add("Stage has no exit code: " + stageName);
                }
                if (!lastExitCode.equals(stageNextExitCode.get(key))) {
                  testConfiguration.failedAsserts.add(
                      "Unexpected stage exit code "
                          + lastExitCode
                          + " and not "
                          + stageNextExitCode.get(key)
                          + ": "
                          + stageName);
                }
                stageLastExitCode.put(key, lastExitCode);

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

                // Change the executor to return the next exit code.
                if (!nextExitCode.equals("")) {
                  if (stage.getExecutor() instanceof KubernetesExecutor) {
                    KubernetesExecutor executor = (KubernetesExecutor) stage.getExecutor();
                    executor.setImageArgs(
                        testConfiguration.nextImageArgs(pipelineName, processId, stageName));
                  } else if (stage.getExecutor() instanceof AbstractLsfExecutor<?>) {
                    AbstractLsfExecutor<?> executor = (AbstractLsfExecutor<?>) stage.getExecutor();
                    executor.setCmd(testConfiguration.nextCmd(pipelineName, processId, stageName));
                  } else if (stage.getExecutor() instanceof CmdExecutor<?>) {
                    CmdExecutor<?> executor = (CmdExecutor<?>) stage.getExecutor();
                    executor.setCmd(testConfiguration.nextCmd(pipelineName, processId, stageName));
                  } else if (stage.getExecutor() instanceof TestExecutor) {
                    // Do nothing
                  } else {
                    log.atSevere().log(
                        "Unexpected executor when spying stage service end execution");
                  }
                }
              } catch (Exception ex) {
                log.atSevere().withCause(ex).log(
                    "Unexpected exception when spying stage service end execution");
              }
              return stageEntity;
            })
        .when(stageServiceSpy)
        .endExecution(any(), any());
  }

  private static TestTypeConfiguration testConfiguration(StageMapKey key) {
    TestTypeConfiguration testConfiguration = stageTestConfiguration.get(key);
    if (testConfiguration == null) {
      throw new RuntimeException("Stage has not been registered");
    }
    return testConfiguration;
  }
}
