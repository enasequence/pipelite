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
package pipelite.runner.pipeline;

import pipelite.Pipeline;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.metrics.PipeliteMetrics;
import pipelite.runner.process.ProcessQueue;
import pipelite.runner.process.ProcessQueueFactory;
import pipelite.runner.process.ProcessRunner;
import pipelite.runner.process.ProcessRunnerFactory;
import pipelite.runner.process.creator.ProcessEntityCreator;
import pipelite.service.PipeliteServices;
import pipelite.service.RegisteredPipelineService;

public class PipelineRunnerFactory {

  private PipelineRunnerFactory() {}

  public static PipelineRunner create(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      String pipelineName) {

    // Get registered pipeline.
    RegisteredPipelineService registeredPipelineService = pipeliteServices.registeredPipeline();
    Pipeline pipeline =
        registeredPipelineService.getRegisteredPipeline(pipelineName, Pipeline.class);
    if (pipeline == null) {
      throw new PipeliteException("Missing pipeline: " + pipelineName);
    }

    // Get process creator.
    ProcessEntityCreator processEntityCreator =
        new ProcessEntityCreator(pipeline, pipeliteServices.process());

    ProcessQueueFactory processQueueFactory =
        (pipeline1) ->
            new ProcessQueue(
                pipeliteConfiguration, pipeliteServices, processEntityCreator, pipeline1);

    boolean lockProcess = true;
    ProcessRunnerFactory processRunnerFactory =
        (pipelineName1, process1) ->
            new ProcessRunner(
                pipeliteConfiguration,
                pipeliteServices,
                pipeliteMetrics,
                pipelineName1,
                process1,
                lockProcess);

    return create(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        pipeline,
        processQueueFactory,
        processRunnerFactory);
  }

  public static PipelineRunner create(
      PipeliteConfiguration pipeliteConfiguration,
      PipeliteServices pipeliteServices,
      PipeliteMetrics pipeliteMetrics,
      Pipeline pipeline,
      ProcessQueueFactory processQueueFactory,
      ProcessRunnerFactory processRunnerFactory) {
    return new PipelineRunner(
        pipeliteConfiguration,
        pipeliteServices,
        pipeliteMetrics,
        pipeline,
        processQueueFactory,
        processRunnerFactory);
  }
}
