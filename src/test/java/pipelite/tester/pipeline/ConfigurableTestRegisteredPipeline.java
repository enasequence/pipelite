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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.RegisteredPipeline;
import pipelite.process.builder.ProcessBuilder;
import pipelite.tester.process.TestProcessConfiguration;

public class ConfigurableTestRegisteredPipeline<T extends TestProcessConfiguration>
    implements RegisteredPipeline {

  private final T testProcessConfiguration;
  private final Set<String> configuredProcessIds = ConcurrentHashMap.newKeySet();

  public ConfigurableTestRegisteredPipeline(T testProcessConfiguration) {
    this.testProcessConfiguration = testProcessConfiguration;
  }

  public T testProcessConfiguration() {
    return testProcessConfiguration;
  }

  @Override
  public final String pipelineName() {
    return testProcessConfiguration.pipelineName();
  }

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    configuredProcessIds.add(builder.getProcessId());
    testProcessConfiguration.configureProcess(builder);
  }

  public Set<String> configuredProcessIds() {
    return configuredProcessIds;
  }

  public int configuredProcessCount() {
    return configuredProcessIds.size();
  }
}
