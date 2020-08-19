package pipelite;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.service.PipeliteProcessService;
import pipelite.service.PipeliteStageService;
import pipelite.service.PipeliteLockService;
import uk.ac.ebi.ena.sra.pipeline.launcher.Launcher;

import javax.transaction.Transactional;

@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired ApplicationConfiguration applicationConfiguration;

  @Autowired PipeliteProcessService pipeliteProcessService;
  @Autowired PipeliteStageService pipeliteStageService;

  @Autowired PipeliteLockService locker;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  @Transactional
  public void run(String... args) {
    Launcher launcher =
        new Launcher(
            applicationConfiguration, pipeliteProcessService, pipeliteStageService, locker);
    launcher.run(args);
  }
}
