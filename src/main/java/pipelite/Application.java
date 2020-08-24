package pipelite;

import lombok.extern.flogger.Flogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.log.LogKey;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.service.PipeliteLockService;
import pipelite.launcher.PipeliteLauncher;

import org.springframework.transaction.annotation.Transactional;

@Flogger
@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired PipeliteLauncher pipeliteLauncher;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  @Transactional
  public void run(String... args) {
    try {
      _run(args);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Uncaught exception");
      throw ex;
    }
  }

  private void _run(String... args) {
    String launcherName = launcherConfiguration.getLauncherName();
    String processName = processConfiguration.getProcessName();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    pipeliteLauncher.stop();
                  } catch (RuntimeException ex) {
                    log.atSevere()
                        .with(LogKey.LAUNCHER_NAME, launcherName)
                        .with(LogKey.PROCESS_NAME, processName)
                        .withCause(ex)
                        .log("Error stopping launcher");
                  }
                }));

    if (!pipeliteLauncher.init()) {
      throw new RuntimeException("Launcher " + launcherName + " could not be started");
    }
    try {
      pipeliteLauncher.execute();
    } finally {
      pipeliteLauncher.stop();
    }
  }
}
