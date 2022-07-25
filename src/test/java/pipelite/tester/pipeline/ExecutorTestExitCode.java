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
package pipelite.tester.pipeline;

import java.util.Arrays;
import java.util.List;
import pipelite.process.builder.ProcessBuilder;
import pipelite.process.builder.StageBuilder;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;

public class ExecutorTestExitCode {

  public static final String IMAGE = "debian:10.11";

  public static ProcessBuilder withSimpleLsfExecutor(
      StageBuilder stageBuilder, int exitCode, SimpleLsfExecutorParameters executorParams) {
    return stageBuilder.withSimpleLsfExecutor(cmdAsString(exitCode), executorParams);
  }

  public static ProcessBuilder withKubernetesExecutor(
      StageBuilder stageBuilder, int exitCode, KubernetesExecutorParameters executorParams) {
    return stageBuilder.withKubernetesExecutor(IMAGE, cmdAsArray(exitCode), executorParams);
  }

  public static ProcessBuilder withCmdExecutor(
      StageBuilder stageBuilder, int exitCode, CmdExecutorParameters executorParams) {
    return stageBuilder.withCmdExecutor(cmdAsString(exitCode), executorParams);
  }

  public static String cmdAsString(int exitCode) {
    return "bash -c 'exit " + exitCode + "'";
  }

  public static List<String> cmdAsArray(int exitCode) {
    return Arrays.asList("bash", "-c", "exit " + exitCode);
  }
}
