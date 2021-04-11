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
package pipelite.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PipeliteConfiguration {
  private final ServiceConfiguration service;
  private final AdvancedConfiguration advanced;
  private final ExecutorConfiguration executor;

  public PipeliteConfiguration(
      @Autowired ServiceConfiguration service,
      @Autowired AdvancedConfiguration advanced,
      @Autowired ExecutorConfiguration executor) {
    this.service = service;
    this.advanced = advanced;
    this.executor = executor;
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
}
