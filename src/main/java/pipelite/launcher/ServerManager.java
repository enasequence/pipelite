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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;

@Flogger
public class ServerManager {

  private ServerManager() {}

  public static final int FORCE_STOP_WAIT_SECONDS = 30;

  private static void forceStop(ServiceManager manager, String serviceName) {
    log.atInfo().with(LogKey.SERVICE_NAME, serviceName).log("Stopping service");
    try {
      manager.stopAsync().awaitStopped(FORCE_STOP_WAIT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException timeout) {
    }
  }

  public static void run(Service service, String serviceName) {
    ServiceManager manager = new ServiceManager(Collections.singleton(service));
    manager.addListener(
        new ServiceManager.Listener() {
          public void stopped() {}

          public void healthy() {}

          public void failure(Service service) {
            log.atSevere()
                .with(LogKey.SERVICE_NAME, serviceName)
                .withCause(service.failureCause())
                .log("Service has failed");
            forceStop(manager, serviceName);
          }
        },
        MoreExecutors.directExecutor());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  forceStop(manager, serviceName);
                }));

    log.atInfo().with(LogKey.SERVICE_NAME, serviceName).log("Starting service");

    manager.startAsync().awaitStopped();

    log.atInfo().with(LogKey.SERVICE_NAME, serviceName).log("Service has stopped");
  }
}
