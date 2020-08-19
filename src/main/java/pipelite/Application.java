package pipelite;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.repository.PipeliteProcessRepository;
import pipelite.repository.PipeliteStageRepository;
import pipelite.service.PipeliteLockService;
import uk.ac.ebi.ena.sra.pipeline.launcher.Launcher;

import javax.transaction.Transactional;

@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired ApplicationConfiguration applicationConfiguration;

  @Autowired PipeliteProcessRepository pipeliteProcessRepository;
  @Autowired PipeliteStageRepository pipeliteStageRepository;

  @Autowired PipeliteLockService locker;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  @Transactional
  public void run(String... args) {
    Launcher launcher =
        new Launcher(
            applicationConfiguration, pipeliteProcessRepository, pipeliteStageRepository, locker);
    launcher.run(args);
  }
}
