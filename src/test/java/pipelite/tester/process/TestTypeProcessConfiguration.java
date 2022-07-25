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
package pipelite.tester.process;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.process.builder.ProcessBuilder;

/**
 * Process configuration for TestType based tests for the configurable test pipeline and schedule.
 */
public abstract class TestTypeProcessConfiguration extends TestProcessConfiguration {

  private final Set<String> configuredProcessIds = ConcurrentHashMap.newKeySet();

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    register(builder.getProcessId());
    configuredProcessIds.add(builder.getProcessId());
    configure(builder);
  }

  protected abstract void configure(ProcessBuilder builder);

  protected final int configuredProcessCount() {
    return configuredProcessIds.size();
  }

  protected final Collection<String> configuredProcessIds() {
    return configuredProcessIds;
  }

  protected void register(String processId) {}
}
