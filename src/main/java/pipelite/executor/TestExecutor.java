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
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.time.Time;

/** Test executor. */
public class TestExecutor extends AbstractExecutor<ExecutorParameters> {

  private final TestExecutorType executorType;
  private final StageExecutorState executorState;
  private final Function<StageExecutorRequest, StageExecutorResult> callback;
  private ZonedDateTime startTime;
  private final Duration executionTime;

  private enum TestExecutorType {
    ASYNC_EXECUTOR,
    SYNC_EXECUTOR
  }

  /**
   * Creates a synchronous test executor that returns the given stage stage.
   *
   * @param executorState the state returned by the executor
   * @return a synchronous test executor that returns the given stage stage
   */
  public static TestExecutor sync(StageExecutorState executorState) {
    return new TestExecutor(TestExecutorType.SYNC_EXECUTOR, executorState, null);
  }
  /**
   * Creates a synchronous test executor that returns the given stage stage.
   *
   * @param executorState the state returned by the executor
   * @paran executionTime the stage execution time
   * @return a synchronous test executor that returns the given stage stage
   */
  public static TestExecutor sync(StageExecutorState executorState, Duration executionTime) {
    return new TestExecutor(TestExecutorType.SYNC_EXECUTOR, executorState, executionTime);
  }

  /**
   * Creates an asynchronous test executor that returns the given stage stage.
   *
   * @param executorState the state returned by the executor
   * @return an asynchronous test executor that returns the given stage stage
   */
  public static TestExecutor async(StageExecutorState executorState) {
    return new TestExecutor(TestExecutorType.ASYNC_EXECUTOR, executorState, null);
  }

  /**
   * Creates an asynchronous test executor that returns the given stage stage.
   *
   * @param executorState the state returned by the executor
   * @paran executionTime the stage execution time
   * @return an asynchronous test executor that returns the given stage stage
   */
  public static TestExecutor async(StageExecutorState executorState, Duration executionTime) {
    return new TestExecutor(TestExecutorType.ASYNC_EXECUTOR, executorState, executionTime);
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
      TestExecutorType executorType, StageExecutorState executorState, Duration executionTime) {
    Assert.notNull(executorType, "Missing executorType");
    Assert.notNull(executorState, "Missing executorState");
    this.executorType = executorType;
    this.executorState = executorState;
    this.callback = null;
    this.executionTime = executionTime;
  }

  private TestExecutor(
      TestExecutorType executorType, Function<StageExecutorRequest, StageExecutorResult> callback) {
    Assert.notNull(executorType, "Missing executorType");
    Assert.notNull(callback, "Missing callback");
    this.executorType = executorType;
    this.executorState = null;
    this.callback = callback;
    this.executionTime = null;
  }

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (executorType == TestExecutorType.SYNC_EXECUTOR) {
      if (callback != null) {
        return callback.apply(request);
      } else {
        if (executionTime != null) {
          Time.wait(executionTime);
        }
        return StageExecutorResult.from(executorState);
      }
    } else {
      boolean isFirstExecution = startTime == null;
      if (isFirstExecution) {
        startTime = ZonedDateTime.now();
        return StageExecutorResult.submitted();
      }
      if (executionTime != null) {
        if (ZonedDateTime.now().isBefore(startTime.plus(executionTime))) {
          return StageExecutorResult.active();
        }
      }
      if (callback != null) {
        return callback.apply(request);
      } else {
        return StageExecutorResult.from(executorState);
      }
    }
  }

  @Override
  public void terminate() {}
}
