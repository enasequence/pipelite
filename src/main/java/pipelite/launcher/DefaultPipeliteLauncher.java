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

import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.launcher.process.creator.DefaultProcessCreator;
import pipelite.launcher.process.creator.ProcessCreator;
import pipelite.launcher.process.queue.DefaultProcessQueue;
import pipelite.launcher.process.queue.ProcessQueue;
import pipelite.launcher.process.runner.DefaultProcessRunner;
import pipelite.launcher.process.runner.DefaultProcessRunnerPool;
import pipelite.lock.DefaultPipeliteLocker;
import pipelite.lock.PipeliteLocker;
import pipelite.process.ProcessFactory;
import pipelite.service.*;

public class DefaultPipeliteLauncher {

  private DefaultPipeliteLauncher() {}

  public static PipeliteLauncher create(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      LockService lockService,
      ProcessFactoryService processFactoryService,
      ProcessSourceService processSourceService,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      String pipelineName) {

    PipeliteLocker pipeliteLocker = new DefaultPipeliteLocker(lockService);
    ProcessFactory processFactory = processFactoryService.create(pipelineName);
    ProcessCreator processCreator =
        new DefaultProcessCreator(
            processSourceService.create(pipelineName), processService, pipelineName);
    ProcessQueue processQueue =
        new DefaultProcessQueue(launcherConfiguration, processService, pipelineName);
    return new PipeliteLauncher(
        launcherConfiguration,
        pipeliteLocker,
        processFactory,
        processCreator,
        processQueue,
        () ->
            new DefaultProcessRunnerPool(
                pipeliteLocker,
                () ->
                    new DefaultProcessRunner(
                        launcherConfiguration,
                        stageConfiguration,
                        processService,
                        stageService,
                        mailService)));
  }
}