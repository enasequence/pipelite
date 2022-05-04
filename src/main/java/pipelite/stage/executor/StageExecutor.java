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
package pipelite.stage.executor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import pipelite.exception.PipeliteException;
import pipelite.executor.*;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileSavePolicy;

/** Executes a stage. Must be serializable to json. */
public interface StageExecutor<T extends ExecutorParameters> {

  /**
   * Returns the executor parameters class.
   *
   * @return the executor parameters class
   */
  Class<T> getExecutorParamsType();

  /**
   * Returns the executor parameters.
   *
   * @return the executor parameters
   */
  T getExecutorParams();

  /**
   * Sets the executor parameters.
   *
   * @param executorParams the executor parameters
   */
  void setExecutorParams(T executorParams);

  /**
   * Prepares the stage for execution.
   *
   * @param pipeliteServices the pipelite services
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stage the stage
   */
  void prepareExecution(
      PipeliteServices pipeliteServices, String pipelineName, String processId, Stage stage);

  /**
   * Called repeatedly to execute the stage until it is not ACTIVE.
   *
   * @param resultCallback execution result callback
   */
  void execute(StageExecutorResultCallback resultCallback);

  /**
   * Returns true if the execution has been submitted to the execution backed.
   *
   * @return true if the execution has been submitted to the execution backend.
   */
  @JsonIgnore
  boolean isSubmitted();

  /** Terminates the stage execution. */
  void terminate();

  /** Resets asynchronous executor state. */
  static void resetAsyncExecutorState(Stage stage) {
    StageExecutor executor = stage.getExecutor();
    if (executor instanceof LsfExecutor) {
      stage.setExecutor(resetLsfExecutorState((LsfExecutor) executor));
    } else if (executor instanceof SimpleLsfExecutor) {
      stage.setExecutor(resetSimpleLsfExecutorState((SimpleLsfExecutor) executor));
    } else if (executor instanceof KubernetesExecutor) {
      stage.setExecutor(resetKubernetesExecutorState((KubernetesExecutor) executor));
    } else if (executor instanceof AwsBatchExecutor) {
      stage.setExecutor(resetAwsBatchExecutorState((AwsBatchExecutor) executor));
    } else if (executor instanceof AsyncTestExecutor) {
      stage.setExecutor(resetAsyncTestExecutorState((AsyncTestExecutor) executor));
    } else {
      throw new PipeliteException(
          "Failed to reset async executor: " + executor.getClass().getSimpleName());
    }
  }

  /**
   * Creates a command executor that executes commands locally or on a remote host using ssh.
   *
   * @param cmd the command
   * @return the command executor
   */
  static CmdExecutor<CmdExecutorParameters> createCmdExecutor(String cmd) {
    CmdExecutor<CmdExecutorParameters> cmdExecutor = new CmdExecutor<>();
    cmdExecutor.setCmd(cmd);
    return cmdExecutor;
  }

  /**
   * Creates an executor that executes the command using LSF locally or on a remote host using ssh.
   *
   * @param cmd the command
   * @return the command executor
   */
  static LsfExecutor createLsfExecutor(String cmd) {
    LsfExecutor lsfExecutor = new LsfExecutor();
    lsfExecutor.setCmd(cmd);
    return lsfExecutor;
  }

  private static LsfExecutor resetLsfExecutorState(LsfExecutor oldExecutor) {
    LsfExecutor newExecutor = StageExecutor.createLsfExecutor(oldExecutor.getCmd());
    newExecutor.setExecutorParams(oldExecutor.getExecutorParams());
    return newExecutor;
  }

  /**
   * Creates an executor that executes the command using LSF locally or on a remote host using ssh.
   *
   * @param cmd the command
   * @return the command executor
   */
  static SimpleLsfExecutor createSimpleLsfExecutor(String cmd) {
    SimpleLsfExecutor lsfExecutor = new SimpleLsfExecutor();
    lsfExecutor.setCmd(cmd);
    return lsfExecutor;
  }

  private static SimpleLsfExecutor resetSimpleLsfExecutorState(SimpleLsfExecutor oldExecutor) {
    SimpleLsfExecutor newExecutor = StageExecutor.createSimpleLsfExecutor(oldExecutor.getCmd());
    newExecutor.setExecutorParams(oldExecutor.getExecutorParams());
    return newExecutor;
  }

  /**
   * Creates an executor that executes the command using Kubernetes.
   *
   * @param image the image
   * @param imageArgs the image arguments
   * @return the command executor
   */
  static KubernetesExecutor createKubernetesExecutor(String image, List<String> imageArgs) {
    KubernetesExecutor kubernetesExecutor = new KubernetesExecutor();
    kubernetesExecutor.setImage(image);
    kubernetesExecutor.setImageArgs(imageArgs);
    return kubernetesExecutor;
  }

  private static KubernetesExecutor resetKubernetesExecutorState(
      KubernetesExecutor kubernetesExecutor) {
    KubernetesExecutor resetExecutor =
        StageExecutor.createKubernetesExecutor(
            kubernetesExecutor.getImage(), kubernetesExecutor.getImageArgs());
    resetExecutor.setExecutorParams(kubernetesExecutor.getExecutorParams());
    return resetExecutor;
  }

  /**
   * Creates an executor that uses AWSBatch.
   *
   * @return the command executor
   */
  static AwsBatchExecutor createAwsBatchExecutor() {
    return new AwsBatchExecutor();
  }

  private static AwsBatchExecutor resetAwsBatchExecutorState(AwsBatchExecutor oldExecutor) {
    AwsBatchExecutor newExecutor = StageExecutor.createAwsBatchExecutor();
    newExecutor.setExecutorParams(oldExecutor.getExecutorParams());
    return newExecutor;
  }

  /**
   * Creates an asynchronous test executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @paran submitTime the stage submit time
   * @paran executionTime the stage execution time
   * @return an asynchronous test executor that returns the given stage state
   */
  static AsyncTestExecutor createAsyncTestExecutor(
      StageExecutorState executorState, Duration submitTime, Duration executionTime) {
    return new AsyncTestExecutor(
        (request) -> StageExecutorResult.from(executorState), submitTime, executionTime);
  }

  /**
   * Creates an asynchronous test executor that executes the given callback.
   *
   * @param callback the callback to execute
   * @paran submitTime the stage submit time
   * @paran executionTime the stage execution time
   * @return an asynchronous test executor that executes the given callback
   */
  static AsyncTestExecutor createAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback,
      Duration submitTime,
      Duration executionTime) {
    return new AsyncTestExecutor(callback, submitTime, executionTime);
  }

  private static AsyncTestExecutor resetAsyncTestExecutorState(AsyncTestExecutor oldExecutor) {
    AsyncTestExecutor newExecutor =
        createAsyncTestExecutor(
            oldExecutor.getCallback(), oldExecutor.getSubmitTime(), oldExecutor.getExecutionTime());
    newExecutor.setExecutorParams(oldExecutor.getExecutorParams());
    return newExecutor;
  }

  /**
   * Creates a synchronous test executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @paran executionTime the stage execution time
   * @return a synchronous test executor that returns the given stage state
   */
  static SyncTestExecutor createSyncTestExecutor(
      StageExecutorState executorState, Duration executionTime) {
    return new SyncTestExecutor(
        (request) -> StageExecutorResult.from(executorState), executionTime);
  }

  /**
   * Creates a synchronous test executor that executes the given callback.
   *
   * @param callback the callback to execute
   * @paran executionTime the stage execution time
   * @return a synchronous test executor that executes the given callback
   */
  static SyncTestExecutor createSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, Duration executionTime) {
    return new SyncTestExecutor(callback, executionTime);
  }

  /**
   * Returns true if the concatenated stdout and stderr output of stage execution should be saved in
   * pipelite database.
   *
   * @param result the stage execution result
   * @return true if the concatenated stdout and stderr output of stage execution should be saved in
   *     pipelite database
   */
  default boolean isSaveLogFile(StageExecutorResult result) {
    return LogFileSavePolicy.isSave(getExecutorParams().getLogSave(), result);
  }
}
