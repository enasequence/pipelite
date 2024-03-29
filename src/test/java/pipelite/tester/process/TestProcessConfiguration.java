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

import pipelite.process.builder.ProcessBuilder;
import pipelite.test.PipeliteTestIdCreator;

/** Process configuration for the configurable test pipeline and schedule. */
public abstract class TestProcessConfiguration {

  private final String pipelineName;

  public TestProcessConfiguration() {
    pipelineName = PipeliteTestIdCreator.pipelineName();
  }

  public TestProcessConfiguration(String pipelineName) {
    this.pipelineName = pipelineName;
  }

  public final String pipelineName() {
    return pipelineName;
  }

  public abstract void configureProcess(ProcessBuilder builder);
}
