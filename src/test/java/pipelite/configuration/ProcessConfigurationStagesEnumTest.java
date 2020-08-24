package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import pipelite.EmptyTestConfiguration;
import pipelite.instance.TaskInstance;
import pipelite.stage.Stage;
import pipelite.task.Task;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    classes = EmptyTestConfiguration.class,
    properties = {
      "pipelite.process.stages=pipelite.configuration.ProcessConfigurationStagesEnumTest$TestStages"
    })
@EnableConfigurationProperties(value = {ProcessConfiguration.class})
public class ProcessConfigurationStagesEnumTest {

  @Autowired ProcessConfiguration config;

  public enum TestStages implements Stage {
    STAGE_1(TestTask.class),
    STAGE_2(TestTask.class);

    TestStages(Class<? extends TestTask> taskClass) {
      this.taskClass = taskClass;
    }

    private final Class<? extends TestTask> taskClass;

    @Override
    public String getStageName() {
      return this.name();
    }

    @Override
    public Class<? extends TestTask> getTaskClass() {
      return taskClass;
    }

    @Override
    public TestStages getDependsOn() {
      return null;
    }

    @Override
    public TaskConfiguration getTaskConfiguration() {
      return new TaskConfiguration();
    }
  }

  public static class TestTask implements Task {

    @Override
    public void execute(TaskInstance taskInstance) {}
  }

  @Test
  public void test() {
    assertThat(config.getStages())
        .isEqualTo("pipelite.configuration.ProcessConfigurationStagesEnumTest$TestStages");
    assertThat(config.getStageArray().length).isEqualTo(2);
    assertThat(config.getStage(TestStages.STAGE_1.name())).isEqualTo(TestStages.STAGE_1);
    assertThat(config.getStage(TestStages.STAGE_2.name())).isEqualTo(TestStages.STAGE_2);
  }
}
