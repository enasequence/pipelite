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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pipelite.tester.TestType.*;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import pipelite.executor.SimpleLsfExecutor;
import pipelite.stage.Stage;
import pipelite.test.PipeliteTestIdCreator;
import pipelite.tester.pipeline.ExecutorTestExitCode;

public class TestTypeTest {

  @Test
  public void noMoreRetries() {
    assertThat(TestType.noMoreRetries(0, 0)).isFalse();
    assertThat(TestType.noMoreRetries(1, 0)).isTrue();

    assertThat(TestType.noMoreRetries(0, 1)).isFalse();
    assertThat(TestType.noMoreRetries(1, 1)).isFalse();
    assertThat(TestType.noMoreRetries(2, 1)).isTrue();

    assertThat(TestType.noMoreRetries(0, 2)).isFalse();
    assertThat(TestType.noMoreRetries(1, 2)).isFalse();
    assertThat(TestType.noMoreRetries(2, 2)).isFalse();
    assertThat(TestType.noMoreRetries(3, 2)).isTrue();
  }

  @Test
  public void expectedStageFailedCnt() {
    assertThat(TestType.expectedStageFailedCnt(Arrays.asList(TestType.EXIT_CODE_SUCCESS), 0))
        .isEqualTo(0);
    assertThat(TestType.expectedStageFailedCnt(Arrays.asList(TestType.EXIT_CODE_SUCCESS), 1))
        .isEqualTo(0);

    assertThat(
            TestType.expectedStageFailedCnt(Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStageFailedCnt(Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR), 1))
        .isEqualTo(1);

    assertThat(TestType.expectedStageFailedCnt(Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR), 0))
        .isEqualTo(1);
    assertThat(TestType.expectedStageFailedCnt(Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR), 1))
        .isEqualTo(1);

    assertThat(
            TestType.expectedStageFailedCnt(
                Collections.nCopies(4, EXIT_CODE_NON_PERMANENT_ERROR), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStageFailedCnt(
                Collections.nCopies(4, EXIT_CODE_NON_PERMANENT_ERROR), 1))
        .isEqualTo(2);
    assertThat(
            TestType.expectedStageFailedCnt(
                Collections.nCopies(4, EXIT_CODE_NON_PERMANENT_ERROR), 2))
        .isEqualTo(3);
    assertThat(
            TestType.expectedStageFailedCnt(
                Collections.nCopies(4, EXIT_CODE_NON_PERMANENT_ERROR), 3))
        .isEqualTo(4);

    assertThat(
            TestType.expectedStageFailedCnt(
                Collections.nCopies(5, EXIT_CODE_NON_PERMANENT_ERROR), 3))
        .isEqualTo(4);
  }

  @Test
  public void expectedStagePermanentErrorCnt() {
    assertThat(
            TestType.expectedStagePermanentErrorCnt(Arrays.asList(TestType.EXIT_CODE_SUCCESS), 0))
        .isEqualTo(0);
    assertThat(
            TestType.expectedStagePermanentErrorCnt(Arrays.asList(TestType.EXIT_CODE_SUCCESS), 5))
        .isEqualTo(0);

    assertThat(
            TestType.expectedStagePermanentErrorCnt(
                Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStagePermanentErrorCnt(
                Collections.nCopies(5, TestType.EXIT_CODE_PERMANENT_ERROR), 5))
        .isEqualTo(1);

    assertThat(
            TestType.expectedStagePermanentErrorCnt(
                Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR), 0))
        .isEqualTo(0);
    assertThat(
            TestType.expectedStagePermanentErrorCnt(
                Collections.nCopies(5, EXIT_CODE_NON_PERMANENT_ERROR), 5))
        .isEqualTo(0);
  }

  @Test
  public void expectedStageExecutionCnt() {
    assertThat(TestType.expectedStageExecutionCnt(Arrays.asList(TestType.EXIT_CODE_SUCCESS), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStageExecutionCnt(
                Collections.nCopies(5, TestType.EXIT_CODE_SUCCESS), 5))
        .isEqualTo(1);

    assertThat(
            TestType.expectedStageExecutionCnt(
                Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStageExecutionCnt(
                Collections.nCopies(5, TestType.EXIT_CODE_PERMANENT_ERROR), 5))
        .isEqualTo(1);

    assertThat(TestType.expectedStageExecutionCnt(Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStageExecutionCnt(
                Collections.nCopies(5, EXIT_CODE_NON_PERMANENT_ERROR), 5))
        .isEqualTo(5);
    assertThat(
            TestType.expectedStageExecutionCnt(
                Collections.nCopies(5 + 1, EXIT_CODE_NON_PERMANENT_ERROR), 5))
        .isEqualTo(6);
  }

  @Test
  public void expectedStageSuccessCnt() {
    assertThat(TestType.expectedStageSuccessCnt(Arrays.asList(TestType.EXIT_CODE_SUCCESS), 0))
        .isEqualTo(1);
    assertThat(
            TestType.expectedStageSuccessCnt(Collections.nCopies(5, TestType.EXIT_CODE_SUCCESS), 5))
        .isEqualTo(1);

    assertThat(
            TestType.expectedStageSuccessCnt(Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR), 0))
        .isEqualTo(0);
    assertThat(
            TestType.expectedStageSuccessCnt(
                Collections.nCopies(5, TestType.EXIT_CODE_PERMANENT_ERROR), 5))
        .isEqualTo(0);

    assertThat(TestType.expectedStageSuccessCnt(Arrays.asList(EXIT_CODE_NON_PERMANENT_ERROR), 0))
        .isEqualTo(0);
    assertThat(
            TestType.expectedStageSuccessCnt(
                Collections.nCopies(5, EXIT_CODE_NON_PERMANENT_ERROR), 5))
        .isEqualTo(0);

    assertThat(
            TestType.expectedStageSuccessCnt(
                Arrays.asList(TestType.EXIT_CODE_NON_PERMANENT_ERROR, TestType.EXIT_CODE_SUCCESS),
                0))
        .isEqualTo(0);
    assertThat(
            TestType.expectedStageSuccessCnt(
                Arrays.asList(TestType.EXIT_CODE_NON_PERMANENT_ERROR, TestType.EXIT_CODE_SUCCESS),
                1))
        .isEqualTo(1);

    assertThat(
            TestType.expectedStageSuccessCnt(
                Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR, TestType.EXIT_CODE_SUCCESS), 0))
        .isEqualTo(0);
    assertThat(
            TestType.expectedStageSuccessCnt(
                Arrays.asList(TestType.EXIT_CODE_PERMANENT_ERROR, TestType.EXIT_CODE_SUCCESS), 1))
        .isEqualTo(1);
  }

  @Test
  public void exitCode() {
    assertThat(TestType.firstExecutionIsSuccessfulTest().exitCode(0))
        .isEqualTo(String.valueOf(EXIT_CODE_SUCCESS));
    assertThat(TestType.firstExecutionIsSuccessfulTest().exitCode(1)).isEqualTo("");

    assertThat(TestType.firstExecutionIsPermanentErrorTest().exitCode(0))
        .isEqualTo(String.valueOf(EXIT_CODE_PERMANENT_ERROR));
    assertThat(TestType.firstExecutionIsPermanentErrorTest().exitCode(1)).isEqualTo("");

    for (int i = 0; i < DEFAULT_MAXIMUM_RETRIES + 1; i++) {
      assertThat(TestType.nonPermanentErrorUntilMaximumRetriesTest().exitCode(i))
          .isEqualTo(String.valueOf(EXIT_CODE_NON_PERMANENT_ERROR));
    }
    assertThat(
            TestType.nonPermanentErrorUntilMaximumRetriesTest()
                .exitCode(DEFAULT_MAXIMUM_RETRIES + 1))
        .isEqualTo("");

    assertThat(TestType.nonPermanentErrorAndThenSuccessTest().exitCode(0))
        .isEqualTo(String.valueOf(EXIT_CODE_NON_PERMANENT_ERROR));
    assertThat(TestType.nonPermanentErrorAndThenSuccessTest().exitCode(1))
        .isEqualTo(String.valueOf(EXIT_CODE_SUCCESS));
    assertThat(TestType.nonPermanentErrorAndThenSuccessTest().exitCode(2))
        .isEqualTo(String.valueOf(""));

    assertThat(TestType.nonPermanentErrorAndThenPermanentErrorTest().exitCode(0))
        .isEqualTo(String.valueOf(EXIT_CODE_NON_PERMANENT_ERROR));
    assertThat(TestType.nonPermanentErrorAndThenPermanentErrorTest().exitCode(1))
        .isEqualTo(String.valueOf(EXIT_CODE_PERMANENT_ERROR));
    assertThat(TestType.nonPermanentErrorAndThenPermanentErrorTest().exitCode(2)).isEqualTo("");
  }

  @Test
  public void next() {
    TestType.init();
    TestType testType = TestType.nonPermanentErrorAndThenSuccessTest();
    String pipelineName = PipeliteTestIdCreator.pipelineName();
    String processId = PipeliteTestIdCreator.processId();
    String stageName = PipeliteTestIdCreator.stageName();
    testType.register(pipelineName, processId, stageName);

    assertThatThrownBy(() -> testType.lastExitCode(pipelineName, processId, stageName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Stage has no last exit code");

    assertThat(testType.nextExitCode(pipelineName, processId, stageName))
        .isEqualTo(EXIT_CODE_NON_PERMANENT_ERROR);
    assertThat(
            ExecutorTestExitCode.cmdAsString(
                testType.nextExitCode(pipelineName, processId, stageName)))
        .isEqualTo("bash -c 'exit " + EXIT_CODE_NON_PERMANENT_ERROR + "'");
    assertThat(
            ExecutorTestExitCode.cmdAsArray(
                testType.nextExitCode(pipelineName, processId, stageName)))
        .containsExactly("bash", "-c", "exit " + EXIT_CODE_NON_PERMANENT_ERROR);

    assertThat(testType.permanentErrors()).containsExactly(EXIT_CODE_PERMANENT_ERROR);

    SimpleLsfExecutor executor = new SimpleLsfExecutor();
    Stage stage = new Stage(stageName, executor);

    TestType.setLastExitCode(
        String.valueOf(EXIT_CODE_NON_PERMANENT_ERROR), pipelineName, processId, stageName);
    TestType.setNextExitCode(testType, stage, pipelineName, processId, stageName);

    assertThat(testType.lastExitCode(pipelineName, processId, stageName))
        .isEqualTo(EXIT_CODE_NON_PERMANENT_ERROR);
    assertThat(
            ExecutorTestExitCode.cmdAsString(
                testType.lastExitCode(pipelineName, processId, stageName)))
        .isEqualTo("bash -c 'exit " + EXIT_CODE_NON_PERMANENT_ERROR + "'");
    assertThat(
            ExecutorTestExitCode.cmdAsArray(
                testType.lastExitCode(pipelineName, processId, stageName)))
        .containsExactly("bash", "-c", "exit " + EXIT_CODE_NON_PERMANENT_ERROR);

    assertThat(testType.nextExitCode(pipelineName, processId, stageName))
        .isEqualTo(EXIT_CODE_SUCCESS);
    assertThat(
            ExecutorTestExitCode.cmdAsString(
                testType.nextExitCode(pipelineName, processId, stageName)))
        .isEqualTo("bash -c 'exit " + EXIT_CODE_SUCCESS + "'");
    assertThat(
            ExecutorTestExitCode.cmdAsArray(
                testType.nextExitCode(pipelineName, processId, stageName)))
        .containsExactly("bash", "-c", "exit " + EXIT_CODE_SUCCESS);
    assertThat(executor.getCmd()).isEqualTo("bash -c 'exit " + EXIT_CODE_SUCCESS + "'");

    TestType.setLastExitCode(String.valueOf(EXIT_CODE_SUCCESS), pipelineName, processId, stageName);
    TestType.setNextExitCode(testType, stage, pipelineName, processId, stageName);

    assertThat(testType.lastExitCode(pipelineName, processId, stageName))
        .isEqualTo(EXIT_CODE_SUCCESS);
    assertThat(
            ExecutorTestExitCode.cmdAsString(
                testType.lastExitCode(pipelineName, processId, stageName)))
        .isEqualTo("bash -c 'exit " + EXIT_CODE_SUCCESS + "'");
    assertThat(
            ExecutorTestExitCode.cmdAsArray(
                testType.lastExitCode(pipelineName, processId, stageName)))
        .containsExactly("bash", "-c", "exit " + EXIT_CODE_SUCCESS);

    assertThatThrownBy(() -> testType.nextExitCode(pipelineName, processId, stageName))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Stage has no next exit code");
  }
}
