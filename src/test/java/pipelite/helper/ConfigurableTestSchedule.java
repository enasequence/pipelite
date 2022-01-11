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

import pipelite.RegisteredPipeline;
import pipelite.Schedule;
import pipelite.process.builder.ProcessBuilder;

public class ConfigurableTestSchedule<T extends RegisteredPipeline> implements Schedule {

  private final String cron;
  private final T registeredPipeline;

  public ConfigurableTestSchedule(String cron, T registeredPipeline) {
    this.cron = cron;
    this.registeredPipeline = registeredPipeline;
  }

  public String cron() {
    return cron;
  }

  public T getRegisteredPipeline() {
    return registeredPipeline;
  }

  @Override
  public final String pipelineName() {
    return registeredPipeline.pipelineName();
  }

  @Override
  public final void configureProcess(ProcessBuilder builder) {
    registeredPipeline.configureProcess(builder);
  }

  @Override
  public final Options configurePipeline() {
    return new Options().cron(cron);
  }
}
