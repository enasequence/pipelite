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
package pipelite.process.builder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import pipelite.executor.*;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorState;
import pipelite.stage.parameters.*;

public class StageBuilder {
  private final ProcessBuilder processBuilder;
  private final String stageName;
  private final List<String> dependsOnStageNames = new ArrayList<>();

  public StageBuilder(
      ProcessBuilder processBuilder, String stageName, Collection<String> dependsOnStageNames) {
    this.processBuilder = processBuilder;
    this.stageName = stageName;
    this.dependsOnStageNames.addAll(dependsOnStageNames);
  }

  public <T extends ExecutorParameters> ProcessBuilder with(StageExecutor<T> executor, T params) {
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   * @param params the executor parameters
   */
  public ProcessBuilder withCmdExecutor(String cmd, CmdExecutorParameters params) {
    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createCmdExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withCmdExecutor(String cmd) {
    return withCmdExecutor(cmd, new CmdExecutorParameters());
  }

  /**
   * An executor that runs a command line command using LSF locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   * @param params the executor parameters
   */
  public ProcessBuilder withSimpleLsfExecutor(String cmd, SimpleLsfExecutorParameters params) {
    SimpleLsfExecutor executor = StageExecutor.createSimpleLsfExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command using LSF locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withSimpleLsfExecutor(String cmd) {
    return withSimpleLsfExecutor(cmd, new SimpleLsfExecutorParameters());
  }

  /**
   * An executor that runs a command line command using SLURM locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   * @param params the executor parameters
   */
  public ProcessBuilder withSimpleSlurmExecutor(String cmd, SimpleSlurmExecutorParameters params) {
    SimpleSlurmExecutor executor = StageExecutor.createSimpleSlurmExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command using SLURM locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withSimpleSlurmExecutor(String cmd) {
    return withSimpleSlurmExecutor(cmd, new SimpleSlurmExecutorParameters());
  }

  /**
   * An executor that runs a command using Kubernetes.
   *
   * @param image the image
   * @param image the image arguments
   * @param params the executor parameters
   */
  public ProcessBuilder withKubernetesExecutor(
      String image, List<String> imageArgs, KubernetesExecutorParameters params) {
    KubernetesExecutor executor = StageExecutor.createKubernetesExecutor(image, imageArgs);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command using Kubernetes.
   *
   * @param image the image
   * @param params the executor parameters
   */
  public ProcessBuilder withKubernetesExecutor(String image, KubernetesExecutorParameters params) {
    KubernetesExecutor executor =
        StageExecutor.createKubernetesExecutor(image, Collections.emptyList());
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command using Kubernetes.
   *
   * @param image the image
   * @param image the image arguments
   */
  public ProcessBuilder withKubernetesExecutor(String image, List<String> imageArgs) {
    return withKubernetesExecutor(image, imageArgs, new KubernetesExecutorParameters());
  }

  /**
   * An executor that runs a command using Kubernetes.
   *
   * @param image the image
   */
  public ProcessBuilder withKubernetesExecutor(String image) {
    return withKubernetesExecutor(
        image, Collections.emptyList(), new KubernetesExecutorParameters());
  }

  /**
   * An executor that runs using AWSBatch.
   *
   * @param params the executor parameters
   */
  public ProcessBuilder withAwsBatchExecutor(AwsBatchExecutorParameters params) {
    AwsBatchExecutor executor = StageExecutor.createAwsBatchExecutor();
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /** An executor that runs using AWSBatch. */
  public ProcessBuilder withAwsBatchExecutor() {
    return withAwsBatchExecutor(AwsBatchExecutorParameters.builder().build());
  }

  /** An executor that behaves like a synchronous executor that returns the stage state SUCCESS. */
  public ProcessBuilder withSyncTestExecutor() {
    return withSyncTestExecutor(StageExecutorState.SUCCESS);
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   */
  public ProcessBuilder withSyncTestExecutor(StageExecutorState executorState) {
    return withSyncTestExecutor(executorState, null, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      StageExecutorState executorState, ExecutorParameters params) {
    return withSyncTestExecutor(executorState, null, params);
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withSyncTestExecutor(
      StageExecutorState executorState, Duration executionTime) {
    return withSyncTestExecutor(executorState, executionTime, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param executionTime the stage execution time
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      StageExecutorState executorState, Duration executionTime, ExecutorParameters params) {
    SyncTestExecutor executor = StageExecutor.createSyncTestExecutor(executorState, executionTime);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that behaves like a synchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   */
  public ProcessBuilder withSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback) {
    return withSyncTestExecutor(callback, null, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like a synchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, ExecutorParameters params) {
    return withSyncTestExecutor(callback, null, params);
  }

  /**
   * An executor that behaves like a synchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, Duration executionTime) {
    return withSyncTestExecutor(callback, executionTime, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like a synchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param executionTime the stage execution time
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback,
      Duration executionTime,
      ExecutorParameters params) {
    SyncTestExecutor executor = StageExecutor.createSyncTestExecutor(callback, executionTime);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   */
  public ProcessBuilder withAsyncTestExecutor(StageExecutorState executorState) {
    return withAsyncTestExecutor(executorState, null, null, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncTestExecutor(
      StageExecutorState executorState, ExecutorParameters params) {
    return withAsyncTestExecutor(executorState, null, null, params);
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param submitTime the stage submit time
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withAsyncTestExecutor(
      StageExecutorState executorState, Duration submitTime, Duration executionTime) {
    return withAsyncTestExecutor(
        executorState, submitTime, executionTime, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param submitTime the stage submit time
   * @param executionTime the stage execution time
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncTestExecutor(
      StageExecutorState executorState,
      Duration submitTime,
      Duration executionTime,
      ExecutorParameters params) {
    AsyncTestExecutor executor =
        StageExecutor.createAsyncTestExecutor(executorState, submitTime, executionTime);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that behaves like an asynchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   */
  public ProcessBuilder withAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback) {
    return withAsyncTestExecutor(callback, null, null, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like an asynchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, ExecutorParameters params) {
    return withAsyncTestExecutor(callback, null, null, params);
  }

  /**
   * An executor that behaves like an asynchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param submitTime the stage submit time
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback,
      Duration submitTime,
      Duration executionTime) {
    return withAsyncTestExecutor(
        callback, submitTime, executionTime, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like an asynchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param submitTime the stage submit time
   * @param executionTime the stage execution time
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback,
      Duration submitTime,
      Duration executionTime,
      ExecutorParameters params) {
    AsyncTestExecutor executor =
        StageExecutor.createAsyncTestExecutor(callback, submitTime, executionTime);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  private <T extends ExecutorParameters> ProcessBuilder addStage(StageExecutor<T> executor) {
    processBuilder.addStage(new Stage(stageName, executor), dependsOnStageNames);
    return processBuilder;
  }
}
