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

import io.micrometer.core.instrument.MeterRegistry;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.StageConfiguration;
import pipelite.launcher.process.runner.DefaultProcessRunner;
import pipelite.launcher.process.runner.DefaultProcessRunnerPool;
import pipelite.launcher.process.runner.ProcessRunnerType;
import pipelite.lock.DefaultPipeliteLocker;
import pipelite.lock.PipeliteLocker;
import pipelite.service.*;

public class DefaultPipeliteScheduler {

  private DefaultPipeliteScheduler() {}

  public static PipeliteScheduler create(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      LockService lockService,
      ProcessFactoryService processFactoryService,
      ProcessService processService,
      ScheduleService scheduleService,
      StageService stageService,
      MailService mailService,
      MeterRegistry meterRegistry) {

    PipeliteLocker pipeliteLocker =
        new DefaultPipeliteLocker(lockService, ProcessRunnerType.SCHEDULER);

    return new PipeliteScheduler(
        launcherConfiguration,
        pipeliteLocker,
        processFactoryService,
        scheduleService,
        processService,
        () ->
            new DefaultProcessRunnerPool(
                pipeliteLocker,
                () ->
                    new DefaultProcessRunner(
                        launcherConfiguration,
                        stageConfiguration,
                        processService,
                        stageService,
                        mailService)),
        meterRegistry);
  }
}
