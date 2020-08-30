package pipelite;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.server.PipeliteLauncher;
import pipelite.server.ServerManager;

@Flogger
@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired PipeliteLauncher pipeliteLauncher;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  public void run(String... args) {
    try {
      launcher();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Uncaught exception");
      throw ex;
    }
  }

  private void launcher() {
    ServerManager.run(pipeliteLauncher, pipeliteLauncher.serviceName());
  }
}
