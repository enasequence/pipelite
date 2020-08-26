package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.context.annotation.ComponentScan;
import pipelite.EmptyTestConfiguration;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.process.stagesFactoryName=pipelite.configuration.ProcessConfigurationStagesFactoryTest$TestStages"
    })
@EnableConfigurationProperties(
        value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(basePackageClasses = {ProcessConfigurationEx.class})
public class ProcessConfigurationStagesFactoryTest {

  @Autowired
  ProcessConfigurationEx processConfiguration;

  public static class TestStages implements StageFactory {
    @Override
    public Stage[] create() {
      Stage[] stages = {new DefaultStage("STAGE_1"), new DefaultStage("STAGE_2")};
      return stages;
    }
  }

  @Test
  public void test() {
    assertThat(processConfiguration.getStages().length).isEqualTo(2);
    assertThat(getStage(processConfiguration.getStages(), "STAGE_1").getStageName())
        .isEqualTo("STAGE_1");
    assertThat(getStage(processConfiguration.getStages(), "STAGE_2").getStageName())
        .isEqualTo("STAGE_2");
  }

  private Stage getStage(Stage[] stages, String name) {
    for (Stage stage : stages) {
      if (name.equals(stage.getStageName())) {
        return stage;
      }
    }
    return null;
  }
}
