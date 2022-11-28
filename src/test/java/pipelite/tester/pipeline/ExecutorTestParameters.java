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
package pipelite.tester.pipeline;

import java.time.Duration;
import java.util.List;
import pipelite.configuration.properties.KubernetesTestConfiguration;
import pipelite.configuration.properties.LsfTestConfiguration;
import pipelite.configuration.properties.SlurmTestConfiguration;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.KubernetesExecutorParameters;
import pipelite.stage.parameters.SimpleLsfExecutorParameters;
import pipelite.stage.parameters.SimpleSlurmExecutorParameters;

public class ExecutorTestParameters {

  private ExecutorTestParameters() {}

  public static SimpleLsfExecutorParameters simpleLsfParams(
      LsfTestConfiguration testConfiguration,
      int immediateRetries,
      int maximumRetries,
      List<Integer> permanentErrors) {
    SimpleLsfExecutorParameters params = new SimpleLsfExecutorParameters();
    params.setHost(testConfiguration.getHost());
    params.setUser(testConfiguration.getUser());
    params.setLogDir(testConfiguration.getLogDir());
    params.setQueue(testConfiguration.getQueue());
    params.setTimeout(Duration.ofSeconds(180));
    params.setImmediateRetries(immediateRetries);
    params.setMaximumRetries(maximumRetries);
    params.setPermanentErrors(permanentErrors);
    return params;
  }

  public static SimpleSlurmExecutorParameters simpleSlurmParams(
      SlurmTestConfiguration testConfiguration,
      int immediateRetries,
      int maximumRetries,
      List<Integer> permanentErrors) {
    SimpleSlurmExecutorParameters params = new SimpleSlurmExecutorParameters();
    params.setHost(testConfiguration.getHost());
    params.setUser(testConfiguration.getUser());
    params.setLogDir(testConfiguration.getLogDir());
    params.setQueue(testConfiguration.getQueue());
    params.setTimeout(Duration.ofSeconds(180));
    params.setImmediateRetries(immediateRetries);
    params.setMaximumRetries(maximumRetries);
    params.setPermanentErrors(permanentErrors);
    params.setMemory(1);
    params.setCpu(1);
    return params;
  }

  public static KubernetesExecutorParameters kubernetesParams(
      KubernetesTestConfiguration testConfiguration,
      int immediateRetries,
      int maximumRetries,
      List<Integer> permanentErrors) {

    KubernetesExecutorParameters params = new KubernetesExecutorParameters();
    params.setNamespace(testConfiguration.getNamespace());
    params.setTimeout(Duration.ofSeconds(180));
    params.setImmediateRetries(immediateRetries);
    params.setMaximumRetries(maximumRetries);
    params.setPermanentErrors(permanentErrors);
    return params;
  }

  public static CmdExecutorParameters cmdParams(
      int immediateRetries, int maximumRetries, List<Integer> permanentErrors) {
    CmdExecutorParameters params = new CmdExecutorParameters();
    params.setImmediateRetries(immediateRetries);
    params.setMaximumRetries(maximumRetries);
    params.setPermanentErrors(permanentErrors);
    return params;
  }
}
