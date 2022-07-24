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
import pipelite.tester.process.TestProcessConfiguration;

/**
 * Creates scheduled processes for testing purposes. The process configuration is defined by
 * TestProcessConfiguration.
 */
public class ConfigurableTestSchedule<T extends TestProcessConfiguration>
    extends ConfigurableTestRegisteredPipeline<T> implements Schedule {

  private final String cron;

  public ConfigurableTestSchedule(String cron, T testProcessConfiguration) {
    super(testProcessConfiguration);
    this.cron = cron;
  }

  @Override
  public final Options configurePipeline() {
    return new Options().cron(cron);
  }
}
