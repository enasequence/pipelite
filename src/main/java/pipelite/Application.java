package pipelite;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;
import pipelite.process.launcher.DefaultProcessLauncherFactory;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.service.PipeliteLockService;
import pipelite.launcher.PipeliteLauncher;

import javax.transaction.Transactional;

@Slf4j
@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired private LauncherConfiguration launcherConfiguration;
  @Autowired private ProcessConfiguration processConfiguration;
  @Autowired private TaskConfiguration taskConfiguration;
  @Autowired private PipeliteProcessService pipeliteProcessService;
  @Autowired private PipeliteStageService pipeliteStageService;
  @Autowired private PipeliteLockService pipeliteLockService;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  @Transactional
  public void run(String... args) {
    try {
      _run(args);
    } catch (Exception ex) {
      log.error("Exception", ex);
      throw ex;
    }
  }

  private void _run(String... args) {
    String launcherName = launcherConfiguration.getLauncherName();
    String processName = processConfiguration.getProcessName();

    DefaultProcessLauncherFactory processLauncherFactory =
        new DefaultProcessLauncherFactory(
            launcherConfiguration,
            processConfiguration,
            taskConfiguration,
            pipeliteProcessService,
            pipeliteStageService,
            pipeliteLockService);

    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            launcherConfiguration,
            processConfiguration,
            pipeliteProcessService,
            processLauncherFactory);

    if (pipeliteLockService.lockLauncher(launcherName, processName)) {

      try {
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      try {
                        pipeliteLauncher.stop();
                      } catch (RuntimeException ex) {
                        log.error(
                            "Error shutting down pipelite.launcher {} for process {}",
                            launcherName,
                            processName,
                            ex);
                      }
                    }));

        pipeliteLauncher.execute();

      } finally {
        pipeliteLockService.unlockLauncher(launcherName, processName);
      }
    } else {
      throw new RuntimeException(
          "Launcher " + launcherName + " is already locked for process " + processName);
    }
  }
}
