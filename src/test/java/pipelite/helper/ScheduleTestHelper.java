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
package pipelite.helper;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import pipelite.Schedule;
import pipelite.UniqueStringGenerator;
import pipelite.process.builder.ProcessBuilder;

public abstract class ScheduleTestHelper implements Schedule {

  private final String pipelineName;
  private final Set<String> configuredProcessIds = ConcurrentHashMap.newKeySet();

  public ScheduleTestHelper() {
    this(UniqueStringGenerator.randomPipelineName(ConfigureProcessPipelineTestHelper.class));
  }

  public ScheduleTestHelper(String pipelineName) {
    this.pipelineName = pipelineName;
  }

  @Override
  public final String pipelineName() {
    return pipelineName;
  }

  @Override
  public final Options configurePipeline() {
    return new Options().cron(_configureCron());
  }

  public String cron() {
    return _configureCron();
  }

  protected abstract String _configureCron();

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    configuredProcessIds.add(builder.getProcessId());
    _configureProcess(builder);
  }

  protected abstract void _configureProcess(ProcessBuilder builder);

  public int configuredProcessCount() {
    return configuredProcessIds.size();
  }

  public Collection<String> configuredProcessIds() {
    return configuredProcessIds;
  }
}
