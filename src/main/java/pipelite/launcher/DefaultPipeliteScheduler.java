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

import pipelite.configuration.AdvancedConfiguration;
import pipelite.configuration.ExecutorConfiguration;
import pipelite.configuration.ServiceConfiguration;
import pipelite.launcher.process.runner.DefaultProcessRunner;
import pipelite.launcher.process.runner.DefaultProcessRunnerPool;
import pipelite.lock.PipeliteLocker;
import pipelite.metrics.PipeliteMetrics;
import pipelite.service.*;

public class DefaultPipeliteScheduler {

  private DefaultPipeliteScheduler() {}

  public static PipeliteScheduler create(
      ServiceConfiguration serviceConfiguration,
      AdvancedConfiguration advancedConfiguration,
      ExecutorConfiguration executorConfiguration,
      PipeliteLocker pipeliteLocker,
      InternalErrorService internalErrorService,
      RegisteredPipelineService registeredPipelineService,
      ProcessService processService,
      ScheduleService scheduleService,
      StageService stageService,
      MailService mailService,
      PipeliteMetrics metrics) {

    return new PipeliteScheduler(
        serviceConfiguration,
        advancedConfiguration,
        internalErrorService,
        registeredPipelineService,
        scheduleService,
        processService,
        new DefaultProcessRunnerPool(
            serviceConfiguration,
            internalErrorService,
            pipeliteLocker,
            (pipelineName) ->
                new DefaultProcessRunner(
                    serviceConfiguration,
                    advancedConfiguration,
                    executorConfiguration,
                    internalErrorService,
                    processService,
                    stageService,
                    mailService,
                    pipelineName),
            metrics),
        metrics);
  }
}
