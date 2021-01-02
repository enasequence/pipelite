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
package pipelite.example;

import org.springframework.stereotype.Component;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;
import pipelite.process.builder.ProcessBuilder;

@Component
public class TestPipeline implements ProcessFactory {

  public static final String PIPELINE_NAME = "testPipeline";
  public static final int PIPELINE_PARALLELISM = 5;

  @Override
  public String getPipelineName() {
    return PIPELINE_NAME;
  }

  @Override
  public int getPipelineParallelism() {
    return PIPELINE_PARALLELISM;
  }

  @Override
  public Process create(ProcessBuilder builder) {
    return builder.execute("STAGE").withCallExecutor().build();
  }
}
