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
            System.exit(1);
          }
        },
        MoreExecutors.directExecutor());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    manager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
                  } catch (TimeoutException timeout) {
                  }
                }));
    manager.startAsync();

    manager.awaitStopped();
  }
}
