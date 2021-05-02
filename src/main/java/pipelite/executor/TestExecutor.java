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
package pipelite.executor;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.function.Function;
import org.springframework.util.Assert;
import pipelite.exception.PipeliteException;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Test executor. */
public class TestExecutor extends AbstractExecutor<ExecutorParameters> {

  private final TestExecutorType executorType;
  private final StageState stageState;
  private final Function<StageExecutorRequest, StageExecutorResult> callback;
  private ZonedDateTime startTime;
  private final Duration executionTime;
  private boolean complete;

  private enum TestExecutorType {
    ASYNC_EXECUTOR,
    SYNC_EXECUTOR
  }

  /**
   * Creates a synchronous test executor that returns the given stage stage.
   *
   * @param stageState the returned stage state
   * @return a synchronous test executor that returns the given stage stage
   */
  public static TestExecutor sync(StageState stageState) {
    return new TestExecutor(TestExecutorType.SYNC_EXECUTOR, stageState, null);
  }
  /**
   * Creates a synchronous test executor that returns the given stage stage.
   *
   * @param stageState the returned stage state
   * @paran executionTime the stage execution time
   * @return a synchronous test executor that returns the given stage stage
   */
  public static TestExecutor sync(StageState stageState, Duration executionTime) {
    return new TestExecutor(TestExecutorType.SYNC_EXECUTOR, stageState, executionTime);
  }

  /**
   * Creates an asynchronous test executor that returns the given stage stage.
   *
   * @param stageState the returned stage state
   * @return an asynchronous test executor that returns the given stage stage
   */
  public static TestExecutor async(StageState stageState) {
    return new TestExecutor(TestExecutorType.ASYNC_EXECUTOR, stageState, null);
  }

  /**
   * Creates an asynchronous test executor that returns the given stage stage.
   *
   * @param stageState the returned stage state
   * @paran executionTime the stage execution time
   * @return an asynchronous test executor that returns the given stage stage
   */
  public static TestExecutor async(StageState stageState, Duration executionTime) {
    return new TestExecutor(TestExecutorType.ASYNC_EXECUTOR, stageState, executionTime);
  }

  /**
   * Creates a synchronous test executor that executes the given callback.
   *
   * @param callback the callback to execute
   * @return a synchronous test executor that executes the given callback
   */
  public static TestExecutor sync(Function<StageExecutorRequest, StageExecutorResult> callback) {
    return new TestExecutor(TestExecutorType.SYNC_EXECUTOR, callback);
  }

  /**
   * Creates an asynchronous test executor that executes the given callback.
   *
   * @param callback the callback to execute
   * @return an asynchronous test executor that executes the given callback
   */
  public static TestExecutor async(Function<StageExecutorRequest, StageExecutorResult> callback) {
    return new TestExecutor(TestExecutorType.ASYNC_EXECUTOR, callback);
  }

  private TestExecutor(
      TestExecutorType executorType, StageState stageState, Duration executionTime) {
    Assert.notNull(executorType, "Missing executorType");
    Assert.notNull(stageState, "Missing stageState");
    this.executorType = executorType;
    this.stageState = stageState;
    this.callback = null;
    this.executionTime = executionTime;
  }

  private TestExecutor(
      TestExecutorType executorType, Function<StageExecutorRequest, StageExecutorResult> callback) {
    Assert.notNull(executorType, "Missing executorType");
    Assert.notNull(callback, "Missing callback");
    this.executorType = executorType;
    this.stageState = null;
    this.callback = callback;
    this.executionTime = null;
  }

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (complete) {
      throw new PipeliteException("Unexpected test executor execute call");
    }
    if (executorType == TestExecutorType.SYNC_EXECUTOR) {
      complete = true;
      if (callback != null) {
        return callback.apply(request);
      } else {
        if (executionTime != null) {
          Time.wait(executionTime);
        }
        return new StageExecutorResult(stageState);
      }
    } else {
      boolean isFirstExecution = startTime == null;
      if (isFirstExecution) {
        startTime = ZonedDateTime.now();
        return new StageExecutorResult(StageState.ACTIVE);
      }
      if (executionTime != null) {
        if (ZonedDateTime.now().isBefore(startTime.plus(executionTime))) {
          return new StageExecutorResult(StageState.ACTIVE);
        }
      }
      complete = true;
      if (callback != null) {
        return callback.apply(request);
      } else {
        return new StageExecutorResult(stageState);
      }
    }
  }

  @Override
  public void terminate() {}
}
