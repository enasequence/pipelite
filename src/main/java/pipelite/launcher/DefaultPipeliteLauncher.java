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
import pipelite.lock.PipeliteLocker;
import pipelite.service.*;

import java.net.InetAddress;
import java.util.UUID;

public class DefaultPipeliteLauncher extends PipeliteLauncher {

  public DefaultPipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      StageConfiguration stageConfiguration,
      LockService lockService,
      ProcessFactoryService processFactoryService,
      ProcessSourceService processSourceService,
      ProcessService processService,
      StageService stageService,
      MailService mailService,
      String pipelineName) {
    super(
        launcherConfiguration,
        new PipeliteLocker(lockService),
        processFactoryService.create(pipelineName),
        new ProcessCreator(processSourceService.create(pipelineName), processService, pipelineName),
        new ProcessQueue(
            launcherConfiguration,
            processService,
            launcherName(pipelineName, launcherConfiguration),
            pipelineName),
        () ->
            new ProcessLauncherPool(
                () ->
                    new ProcessLauncher(
                        launcherConfiguration,
                        stageConfiguration,
                        processService,
                        stageService,
                        mailService)));
  }

  public static String launcherName(
      String pipelineName, LauncherConfiguration launcherConfiguration) {
    return pipelineName
        + "@"
        + getCanonicalHostName()
        + ":"
        + launcherConfiguration.getPort()
        + ":"
        + UUID.randomUUID();
  }

  public static String getCanonicalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
