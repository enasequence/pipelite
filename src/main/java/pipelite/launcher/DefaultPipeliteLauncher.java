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

import pipelite.Pipeline;
import pipelite.PrioritizedPipeline;
import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.launcher.process.creator.DefaultPrioritizedProcessCreator;
import pipelite.launcher.process.creator.PrioritizedProcessCreator;
import pipelite.launcher.process.queue.DefaultProcessQueue;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.DefaultProcessRunner;
import pipelite.launcher.process.runner.DefaultProcessRunnerPool;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.*;

public class DefaultPipeliteLauncher {

  private DefaultPipeliteLauncher() {}

  public static PipeliteLauncher create(
      ServiceConfiguration serviceConfiguration,
      AdvancedConfiguration advancedConfiguration,
      ExecutorConfiguration executorConfiguration,
      PipeliteLocker pipeliteLocker,
      InternalErrorService internalErrorService,
      RegisteredPipelineService registeredPipelineService,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      PipeliteMetrics metrics,
      String pipelineName) {

    Pipeline pipeline =
        registeredPipelineService.getRegisteredPipeline(pipelineName, Pipeline.class);
    if (pipeline == null) {
      throw new PipeliteException("Missing pipeline: " + pipelineName);
    }
    PrioritizedProcessCreator prioritizedProcessCreator =
        new DefaultPrioritizedProcessCreator(
            registeredPipelineService.getRegisteredPipeline(
                pipelineName, PrioritizedPipeline.class),
            processService);
    ProcessQueue processQueue =
        new DefaultProcessQueue(
            advancedConfiguration,
            processService,
            pipelineName,
            pipeline.configurePipeline().pipelineParallelism());
    return new PipeliteLauncher(
        serviceConfiguration,
        advancedConfiguration,
        internalErrorService,
        pipeline,
        prioritizedProcessCreator,
        processQueue,
        new DefaultProcessRunnerPool(
            pipeliteLocker,
            (pipelineName1) ->
                new DefaultProcessRunner(
                    advancedConfiguration,
                    executorConfiguration,
                    processService,
                    stageService,
                    mailService,
                    pipelineName1),
            metrics),
        metrics);
  }
}
