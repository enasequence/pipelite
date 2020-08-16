package pipelite;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.ac.ebi.ena.sra.pipeline.launcher.Launcher;

@SpringBootApplication
public class Application implements CommandLineRunner {

    @Autowired
    ApplicationConfiguration applicationConfiguration;

  // @Autowired Launcher launcher;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  public void run(String... args) {
    Launcher launcher = new Launcher(applicationConfiguration);
    launcher.run(args);
  }
}
