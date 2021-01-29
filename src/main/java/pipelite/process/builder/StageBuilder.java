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

import static pipelite.stage.StageState.ACTIVE;
import static pipelite.stage.StageState.SUCCESS;

import java.util.*;
import pipelite.executor.*;
import pipelite.stage.Stage;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.executor.StageExecutorCall;
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

  public ProcessBuilder with(StageExecutor executor) {
    return with(executor, ExecutorParameters.builder().build());
  }

  public ProcessBuilder with(StageExecutor executor, ExecutorParameters params) {
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
   * An executor that executes the given call.
   *
   * @param call the call to be executed
   */
  public ProcessBuilder withCallExecutor(StageExecutorCall call) {
    return withCallExecutor(call, ExecutorParameters.builder().build());
  }

  /**
   * An executor that executes the given call.
   *
   * @param call the call to be executed
   * @param params the executor parameters
   */
  public ProcessBuilder withCallExecutor(StageExecutorCall call, ExecutorParameters params) {
    CallExecutor executor = new CallExecutor(call);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * A test executor that behaves like a synchronous executor and returns the given stage state.
   *
   * @param stageState the stage state returned by the executor
   */
  public ProcessBuilder withCallExecutor(StageState stageState) {
    return withCallExecutor(stageState, ExecutorParameters.builder().build());
  }

  /**
   * A test executor that behaves like a synchronous executor and returns the given stage state.
   *
   * @param stageState the stage state returned by the executor
   * @param params the executor parameters
   */
  public ProcessBuilder withCallExecutor(StageState stageState, ExecutorParameters params) {
    CallExecutor executor = new CallExecutor(stageState);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /** A test executor that behaves like a synchronous executor and returns stage state SUCCESS. */
  public ProcessBuilder withCallExecutor() {
    return withCallExecutor(StageState.SUCCESS);
  }

  /**
   * A test executor that behaves like an asynchronous executor by returning ACTIVE stage state when
   * called for the first time and then returning the given stage state for subsequent calls.
   *
   * @param stageState the stage state returned after returning ACTIVE
   */
  public ProcessBuilder withAsyncCallExecutor(StageState stageState) {
    return withAsyncCallExecutor(stageState, ExecutorParameters.builder().build());
  }

  /**
   * A test executor that behaves like an asynchronous executor by returning ACTIVE stage state when
   * called for the first time and then returning the given stage state for subsequent calls.
   *
   * @param stageState the stage state returned after returning ACTIVE
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncCallExecutor(StageState stageState, ExecutorParameters params) {
    CallExecutor executor = new CallExecutor(Arrays.asList(ACTIVE, stageState));
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * A test executor that behaves like an asynchronous executor by returning ACTIVE stage state when
   * called for the first time and then returning the stage state SUCCESS.
   */
  public ProcessBuilder withAsyncCallExecutor() {
    return withAsyncCallExecutor(SUCCESS);
  }

  private ProcessBuilder addStage(StageExecutor executor) {
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
