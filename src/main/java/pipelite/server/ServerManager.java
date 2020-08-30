package pipelite.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Flogger
public class ServerManager {

  // Suppresses default constructor, ensuring non-instantiability.
  private ServerManager() {}

  public static final int FORCE_STOP_WAIT_SECONDS = 30;

  private static void forceStop(ServiceManager manager, Service service, String serviceName) {
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
            forceStop(manager, service, serviceName);
          }
        },
        MoreExecutors.directExecutor());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  forceStop(manager, service, serviceName);
                }));

    log.atInfo().with(LogKey.SERVICE_NAME, serviceName).log("Starting service");

    manager.startAsync().awaitStopped();

    log.atInfo().with(LogKey.SERVICE_NAME, serviceName).log("Service has stopped");
  }
}
