package pipelite;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import uk.ac.ebi.ena.sra.pipeline.launcher.Launcher;

@SpringBootApplication
public class Application implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(Launcher.class, args);
    }

  @Override
  public void run(String... args) {
        Launcher launcher = new Launcher();
        launcher.run(args);
  }
}
