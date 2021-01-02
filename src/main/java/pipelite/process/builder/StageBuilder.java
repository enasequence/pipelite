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

import static pipelite.stage.executor.StageExecutorResultType.ACTIVE;
import static pipelite.stage.executor.StageExecutorResultType.SUCCESS;

import java.util.*;

import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.CallExecutor;
import pipelite.executor.CmdExecutor;
import pipelite.executor.LsfExecutor;
import pipelite.stage.Stage;
import pipelite.stage.executor.*;
import pipelite.stage.parameters.AwsBatchExecutorParameters;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.LsfExecutorParameters;

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
   * An executor that runs a command line command locally.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withLocalCmdExecutor(String cmd) {
    return withLocalCmdExecutor(cmd, new CmdExecutorParameters());
  }

  /**
   * An executor that runs a command line command locally.
   *
   * @param cmd the command line command to execute
   * @oaram params the executor parameters
   */
  public ProcessBuilder withLocalCmdExecutor(String cmd, CmdExecutorParameters params) {
    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createLocalCmdExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command remotely over ssh.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withSshCmdExecutor(String cmd) {
    return withSshCmdExecutor(cmd, new CmdExecutorParameters());
  }

  /**
   * An executor that runs a command line command remotely over ssh.
   *
   * @param cmd the command line command to execute
   * @oaram params the executor parameters
   */
  public ProcessBuilder withSshCmdExecutor(String cmd, CmdExecutorParameters params) {
    CmdExecutor<CmdExecutorParameters> executor = StageExecutor.createSshCmdExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command locally using LSF.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withLocalLsfExecutor(String cmd) {
    return withLocalLsfExecutor(cmd, new LsfExecutorParameters());
  }

  /**
   * An executor that runs a command line command locally using LSF.
   *
   * @param cmd the command line command to execute
   * @oaram params the executor parameters
   */
  public ProcessBuilder withLocalLsfExecutor(String cmd, LsfExecutorParameters params) {
    LsfExecutor executor = StageExecutor.createLocalLsfExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs a command line command remotely over ssh using LSF.
   *
   * @param cmd the command line command to execute
   */
  public ProcessBuilder withSshLsfExecutor(String cmd) {
    return withLocalLsfExecutor(cmd, new LsfExecutorParameters());
  }

  /**
   * An executor that runs a command line command remotely over ssh using LSF.
   *
   * @param cmd the command line command to execute
   * @oaram params the executor parameters
   */
  public ProcessBuilder withSshLsfExecutor(String cmd, LsfExecutorParameters params) {
    LsfExecutor executor = StageExecutor.createSshLsfExecutor(cmd);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that runs using AWSBatch.
   *
   * @oaram params the executor parameters
   */
  public ProcessBuilder withAwsBatchExecutor(AwsBatchExecutorParameters params) {
    AwsBatchExecutor executor = StageExecutor.createAwsBatchExecutor();
    executor.setExecutorParams(params);
    return addStage(executor);
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
   * @oaram params the executor parameters
   */
  public ProcessBuilder withCallExecutor(StageExecutorCall call, ExecutorParameters params) {
    CallExecutor executor = new CallExecutor(call);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that returns a stage execution result of the given result type.
   *
   * @param resultType the stage execution result type returned by the executor
   */
  public ProcessBuilder withCallExecutor(StageExecutorResultType resultType) {
    return withCallExecutor(resultType, ExecutorParameters.builder().build());
  }

  /**
   * An executor that returns a stage execution result of the given result type.
   *
   * @param resultType the stage execution result type returned by the executor
   * @param params the executor parameters
   */
  public ProcessBuilder withCallExecutor(
      StageExecutorResultType resultType, ExecutorParameters params) {
    CallExecutor executor = new CallExecutor(resultType);
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /** An executor that returns the stage execution result SUCCESS. */
  public ProcessBuilder withCallExecutor() {
    return withCallExecutor(StageExecutorResultType.SUCCESS);
  }

  /**
   * An executor that first returns the stage execution result ACTIVE and then a stage execution
   * result of the given result type.
   *
   * @param resultType the stage execution result returned by the executor after ACTIVE
   */
  public ProcessBuilder withAsyncCallExecutor(StageExecutorResultType resultType) {
    return withAsyncCallExecutor(resultType, ExecutorParameters.builder().build());
  }

  /**
   * An executor that first returns the stage execution result ACTIVE and then a stage execution
   * result of the given result type.
   *
   * @param resultType the stage execution result returned by the executor after ACTIVE
   * @param params the executor parameters
   */
  public ProcessBuilder withAsyncCallExecutor(
      StageExecutorResultType resultType, ExecutorParameters params) {
    CallExecutor executor = new CallExecutor(Arrays.asList(ACTIVE, resultType));
    executor.setExecutorParams(params);
    return addStage(executor);
  }

  /**
   * An executor that first returns the stage execution result ACTIVE and then a stage execution
   * result SUCCESS.
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
