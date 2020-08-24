package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;

import pipelite.EmptyTestConfiguration;
import pipelite.instance.TaskInstance;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;
import pipelite.task.Task;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.process.stages=pipelite.configuration.ProcessConfigurationStagesFactoryTest$TestStages"
    })
@EnableConfigurationProperties(value = {ProcessConfiguration.class})
public class ProcessConfigurationStagesFactoryTest {

  @Autowired ProcessConfiguration config;

  public static class TestStages implements StageFactory {
    @Override
    public Stage[] create() {
      Stage[] stages = {
        new DefaultStage("STAGE_1", TestTask.class), new DefaultStage("STAGE_2", TestTask.class)
      };
      return stages;
    }
  }

  public static class TestTask implements Task {
    @Override
    public void execute(TaskInstance taskInstance) {}
  }

  @Test
  public void test() {
    assertThat(config.getStages())
        .isEqualTo("pipelite.configuration.ProcessConfigurationStagesFactoryTest$TestStages");
    assertThat(config.getStageArray().length).isEqualTo(2);
    assertThat(config.getStage("STAGE_1").getStageName()).isEqualTo("STAGE_1");
    assertThat(config.getStage("STAGE_2").getStageName()).isEqualTo("STAGE_2");
  }
}
