package pipelite;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.datasource.DataSourceUtils;
import uk.ac.ebi.ena.sra.pipeline.launcher.Launcher;

import javax.sql.DataSource;
import javax.transaction.Transactional;

@SpringBootApplication
public class Application implements CommandLineRunner {

    @Autowired
    ApplicationConfiguration applicationConfiguration;

    @Autowired
    DataSource dataSource;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  @Transactional
  public void run(String... args) {
    Launcher launcher = new Launcher(applicationConfiguration, DataSourceUtils.getConnection(dataSource));
    launcher.run(args);
  }
}
