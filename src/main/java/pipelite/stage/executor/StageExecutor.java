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

import pipelite.executor.AwsBatchExecutor;
import pipelite.executor.CmdExecutor;
import pipelite.executor.LsfExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.cmd.LocalCmdRunner;
import pipelite.executor.cmd.SshCmdRunner;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;

/** Executes a stage. Must be serializable to json. */
public interface StageExecutor<T extends ExecutorParameters> extends StageExecutorCall {

  Class<T> getExecutorParamsType();

  T getExecutorParams();

  void setExecutorParams(T executorParams);

  /**
   * Creates a command executor.
   *
   * @param cmd the command
   * @param cmdRunner the command runner
   * @return a command executor.
   */
  static CmdExecutor<CmdExecutorParameters> createCmdExecutor(String cmd, CmdRunner cmdRunner) {
    CmdExecutor<CmdExecutorParameters> cmdExecutor = new CmdExecutor<>();
    cmdExecutor.setCmd(cmd);
    cmdExecutor.setCmdRunner(cmdRunner);
    return cmdExecutor;
  }

  /**
   * Creates an executor that executes the command on the local host.
   *
   * @param cmd the command
   * @return an executor that executes the command on the local host.
   */
  static CmdExecutor<CmdExecutorParameters> createLocalCmdExecutor(String cmd) {
    return createCmdExecutor(cmd, new LocalCmdRunner());
  }

  /**
   * Creates an executor that connects to a remote host using ssh and executes the command.
   *
   * @param cmd the command
   * @return an executor that connects to a remote host using ssh and executes the command.
   */
  static CmdExecutor<CmdExecutorParameters> createSshCmdExecutor(String cmd) {
    return createCmdExecutor(cmd, new SshCmdRunner());
  }

  /**
   * Creates an executor that executes the command using LSF.
   *
   * @param cmd the command
   * @param cmdRunner the command runner
   * @return an executor that executes the command using LSF.
   */
  static LsfExecutor createLsfExecutor(String cmd, CmdRunner cmdRunner) {
    LsfExecutor lsfExecutor = new LsfExecutor();
    lsfExecutor.setCmd(cmd);
    lsfExecutor.setCmdRunner(cmdRunner);
    return lsfExecutor;
  }

  /**
   * Creates an executor that executes the command using LSF on the local host.
   *
   * @param cmd the command
   * @return an executor that executes the command using LSF on the local host.
   */
  static LsfExecutor createLocalLsfExecutor(String cmd) {
    return createLsfExecutor(cmd, new LocalCmdRunner());
  }

  /**
   * Creates an executor that connects to a remote host using ssh and executes the command using
   * LSF.
   *
   * @param cmd the command
   * @return an executor that connects to a remote host using ssh and executes the command using
   *     LSF.
   */
  static LsfExecutor createSshLsfExecutor(String cmd) {
    return createLsfExecutor(cmd, new SshCmdRunner());
  }

  /**
   * Creates an executor that uses AWSBatch.
   *
   * @return the executor that uses AWSBatch
   */
  static AwsBatchExecutor createAwsBatchExecutor() {
    return new AwsBatchExecutor();
  }
}
