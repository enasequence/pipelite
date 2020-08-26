package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import pipelite.EmptyTestConfiguration;
import pipelite.stage.Stage;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.process.stagesEnumName=pipelite.configuration.ProcessConfigurationStagesEnumTest$TestStages"
    })
@EnableConfigurationProperties(
        value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(basePackageClasses = {ProcessConfigurationEx.class})
public class ProcessConfigurationStagesEnumTest {

  @Autowired
  ProcessConfigurationEx processConfiguration;

  public enum TestStages implements Stage {
    STAGE_1,
    STAGE_2;

    TestStages() {}

    @Override
    public String getStageName() {
      return this.name();
    }

    @Override
    public TestStages getDependsOn() {
      return null;
    }

    @Override
    public TaskConfigurationEx getTaskConfiguration() {
      return new TaskConfigurationEx(new TaskConfiguration());
    }
  }

  @Test
  public void test() {
    assertThat(processConfiguration.getStages().length).isEqualTo(2);
    assertThat(getStage(processConfiguration.getStages(), TestStages.STAGE_1.name()))
        .isEqualTo(TestStages.STAGE_1);
    assertThat(getStage(processConfiguration.getStages(), TestStages.STAGE_2.name()))
        .isEqualTo(TestStages.STAGE_2);
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
