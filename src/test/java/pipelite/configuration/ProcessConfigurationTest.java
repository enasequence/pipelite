package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.stage.DefaultStages;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class ProcessConfigurationTest {

    @Autowired
    ProcessConfiguration config;

    @Test
    public void test() {
        assertThat(config.getResolver()).isEqualTo("pipelite.resolver.DefaultExceptionResolver");
        assertThat(config.createResolver()).isInstanceOf(DefaultExceptionResolver.class);
        assertThat(config.getJavaProperties().length).isEqualTo(2);
        assertThat(config.getJavaProperties()[0]).isEqualTo("TEST1");
        assertThat(config.getJavaProperties()[1]).isEqualTo("TEST2");
        assertThat(config.getStages()).isEqualTo("pipelite.stage.DefaultStages");
        assertThat(config.getStageArray().length).isEqualTo(2);
        assertThat(config.getStage(DefaultStages.STAGE_1.name())).isEqualTo(DefaultStages.STAGE_1);
        assertThat(config.getStage(DefaultStages.STAGE_2.name())).isEqualTo(DefaultStages.STAGE_2);
    }
}
