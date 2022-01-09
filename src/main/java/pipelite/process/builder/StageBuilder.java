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
package pipelite.process.builder;

import java.time.Duration;
import java.util.*;
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

  public ProcessBuilder with(StageExecutor<ExecutorParameters> executor) {
    return with(executor, ExecutorParameters.builder().build());
  }

  public ProcessBuilder with(
      StageExecutor<ExecutorParameters> executor, ExecutorParameters params) {
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
  public ProcessBuilder withLsfExecutor(String cmd, LsfExecutorParameters params) {
    LsfExecutor executor = StageExecutor.createLsfExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command using LSF locally or on a remote host using ssh.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withLsfExecutor(String cmd) {
    return withLsfExecutor(cmd, new LsfExecutorParameters());
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

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   */
  public ProcessBuilder withSyncTestExecutor(StageExecutorState executorState) {
    return withSyncTestExecutor(executorState, ExecutorParameters.builder().build(), null);
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withSyncTestExecutor(
      StageExecutorState executorState, Duration executionTime) {
    return withSyncTestExecutor(executorState, ExecutorParameters.builder().build(), executionTime);
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      StageExecutorState executorState, ExecutorParameters params) {
    return withSyncTestExecutor(executorState, params, null);
  }

  /**
   * An executor that behaves like a synchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param executionTime the stage execution time
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      StageExecutorState executorState, ExecutorParameters params, Duration executionTime) {
    TestExecutor executor = TestExecutor.sync(executorState, executionTime);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /** An executor that behaves like a synchronous executor that returns the stage state SUCCESS. */
  public ProcessBuilder withSyncTestExecutor() {
    return withSyncTestExecutor(StageExecutorState.SUCCESS);
  }

  /**
   * An executor that behaves like a synchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   */
  public ProcessBuilder withSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback) {
    return withSyncTestExecutor(callback, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like a synchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param params the executor parameters
   */
  public ProcessBuilder withSyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, ExecutorParameters params) {
    TestExecutor executor = TestExecutor.sync(callback);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   */
  public ProcessBuilder withAsyncTestExecutor(StageExecutorState executorState) {
    return withAsyncTestExecutor(executorState, ExecutorParameters.builder().build(), null);
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withAsyncTestExecutor(
      StageExecutorState executorState, Duration executionTime) {
    return withAsyncTestExecutor(
        executorState, ExecutorParameters.builder().build(), executionTime);
  }

  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncTestExecutor(
      StageExecutorState executorState, ExecutorParameters params) {
    return withAsyncTestExecutor(executorState, params, null);
  }
  /**
   * An executor that behaves like an asynchronous executor that returns the given stage state.
   *
   * @param executorState the state returned by the executor
   * @param params the executor parameters
   * @param executionTime the stage execution time
   */
  public ProcessBuilder withAsyncTestExecutor(
      StageExecutorState executorState, ExecutorParameters params, Duration executionTime) {
    TestExecutor executor = TestExecutor.async(executorState, executionTime);
    executor.setExecutorParams(params);
    return addStage(executor);
  }
  /**
   * An executor that behaves like an asynchronous executor that returns the stage state SUCCESS.
   */
  public ProcessBuilder withAsyncTestExecutor() {
    return withAsyncTestExecutor(StageExecutorState.SUCCESS);
  }

  /**
   * An executor that behaves like an asynchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   */
  public ProcessBuilder withAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback) {
    return withAsyncTestExecutor(callback, ExecutorParameters.builder().build());
  }

  /**
   * An executor that behaves like an asynchronous executor that runs the given callback.
   *
   * @param callback the callback to be executed
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncTestExecutor(
      Function<StageExecutorRequest, StageExecutorResult> callback, ExecutorParameters params) {
    TestExecutor executor = TestExecutor.async(callback);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  private <T extends ExecutorParameters> ProcessBuilder addStage(StageExecutor<T> executor) {
    List<Stage> dependsOn = new ArrayList<>();
    for (String dependsOnStageName : dependsOnStageNames) {
      Optional<Stage> dependsOnOptional =
          processBuilder.stages.stream()
              .filter(stage -> stage.getStageName().equals(dependsOnStageName))
              .findFirst();

      if (!dependsOnOptional.isPresent()) {
        throw new IllegalArgumentException("Unknown stage dependency: " + dependsOnStageName);
      }
      dependsOn.add(dependsOnOptional.get());
    }

    processBuilder.stages.add(
        Stage.builder().stageName(stageName).executor(executor).dependsOn(dependsOn).build());
    return processBuilder;
  }
}
