package pipelite.launcher;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Flogger
public class PipeliteLauncherServiceManager {

  // Suppresses default constructor, ensuring non-instantiability.
  private PipeliteLauncherServiceManager() {}

  private static final int FORCE_STOP_WAIT_SECONDS = 5;

  private static void forceStop(ServiceManager manager, PipeliteLauncher pipeliteLauncher) {
    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, pipeliteLauncher.serviceName())
        .log("Stopping pipelite launcher");
    try {
      manager.stopAsync().awaitStopped(FORCE_STOP_WAIT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException timeout) {
    }
  }

  public static void run(PipeliteLauncher pipeliteLauncher) {
    ServiceManager manager = new ServiceManager(Collections.singleton(pipeliteLauncher));
    manager.addListener(
        new ServiceManager.Listener() {
          public void stopped() {}

          public void healthy() {}

          public void failure(Service service) {
            log.atSevere()
                .with(LogKey.LAUNCHER_NAME, pipeliteLauncher.serviceName())
                .log("Pipelite launcher has failed");
            forceStop(manager, pipeliteLauncher);
          }
        },
        MoreExecutors.directExecutor());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  forceStop(manager, pipeliteLauncher);
                }));

    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, pipeliteLauncher.serviceName())
        .log("Starting pipelite launcher");

    manager.startAsync().awaitStopped();

    log.atInfo()
        .with(LogKey.LAUNCHER_NAME, pipeliteLauncher.serviceName())
        .log("Pipelite launcher has stopped");
  }
}
