package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;

import pipelite.EmptyTestConfiguration;
import pipelite.executor.TaskExecutor;
import pipelite.executor.TaskExecutorFactory;
import pipelite.instance.TaskInstance;
import pipelite.stage.DefaultStage;
import pipelite.stage.Stage;
import pipelite.stage.StageFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInfo;

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
        new DefaultStage(
            "STAGE_1", (processConfiguration, taskConfiguration) -> new TestTaskExecutor()),
        new DefaultStage(
            "STAGE_2", (processConfiguration, taskConfiguration) -> new TestTaskExecutor())
      };
      return stages;
    }
  }

  public static class TestTaskExecutor implements TaskExecutor {
    @Override
    public ExecutionInfo execute(TaskInstance instance) {
      return new ExecutionInfo();
    }
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
