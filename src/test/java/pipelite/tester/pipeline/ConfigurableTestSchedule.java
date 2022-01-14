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

import pipelite.Schedule;
import pipelite.process.builder.ProcessBuilder;
import pipelite.tester.process.TestProcessConfiguration;

public class ConfigurableTestSchedule<T extends TestProcessConfiguration> implements Schedule {

  private final String cron;
  private final T testProcessConfiguration;

  public ConfigurableTestSchedule(String cron, T testProcessConfiguration) {
    this.cron = cron;
    this.testProcessConfiguration = testProcessConfiguration;
  }

  public String cron() {
    return cron;
  }

  public T getTestProcessConfiguration() {
    return testProcessConfiguration;
  }

  @Override
  public final String pipelineName() {
    return testProcessConfiguration.pipelineName();
  }

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    testProcessConfiguration.configureProcess(builder);
  }

  @Override
  public final Options configurePipeline() {
    return new Options().cron(cron);
  }
}
