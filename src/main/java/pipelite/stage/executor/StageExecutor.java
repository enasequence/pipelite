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

import java.util.List;
import pipelite.executor.*;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.cmd.LogFileRetentionPolicy;
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
   * Called repeatedly to execute the stage until it is not ACTIVE.
   *
   * @param request the execution request
   * @return stage execution result
   */
  StageExecutorResult execute(StageExecutorRequest request);

  /** Terminates the stage execution. */
  void terminate();

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

  /**
   * Creates an executor that uses AWSBatch.
   *
   * @return the command executor
   */
  static AwsBatchExecutor createAwsBatchExecutor() {
    return new AwsBatchExecutor();
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

  /**
   * Returns true if the concatenated stdout and stderr output of stage execution should be deleted
   * from the working directory.
   *
   * @param result the stage execution result
   * @return true if the concatenated stdout and stderr output of stage execution should be deleted
   *     from the working directory.
   */
  default boolean isDeleteLogFile(StageExecutorResult result) {
    return LogFileRetentionPolicy.isDelete(getExecutorParams().getLogRetention(), result);
  }
}
