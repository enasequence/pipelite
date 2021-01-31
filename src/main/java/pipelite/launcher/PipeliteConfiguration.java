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
package pipelite.launcher;

import org.springframework.util.Assert;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.metrics.PipeliteMetrics;

public class PipeliteConfiguration {
  private final ServiceConfiguration service;
  private final AdvancedConfiguration advanced;
  private final ExecutorConfiguration executor;
  private final PipeliteMetrics metrics;

  public PipeliteConfiguration(
      ServiceConfiguration service,
      AdvancedConfiguration advanced,
      ExecutorConfiguration executor,
      PipeliteMetrics metrics) {
    Assert.notNull(service, "Missing service configuration");
    Assert.notNull(advanced, "Missing advanced configuration");
    Assert.notNull(executor, "Missing executor configuration");
    Assert.notNull(metrics, "Missing metrics configuration");
    this.service = service;
    this.advanced = advanced;
    this.executor = executor;
    this.metrics = metrics;
  }

  public ServiceConfiguration service() {
    return service;
  }

  public AdvancedConfiguration advanced() {
    return advanced;
  }

  public ExecutorConfiguration executor() {
    return executor;
  }

  public PipeliteMetrics metrics() {
    return metrics;
  }
}
