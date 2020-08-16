package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import uk.ac.ebi.ena.sra.pipeline.launcher.Launcher;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = Launcher.class)
@ActiveProfiles("test")
@EnableConfigurationProperties(value = LSFTaskExecutorConfiguration.class)
public class LSFTaskExecutorConfigurationTest {

    @Autowired
    LSFTaskExecutorConfiguration config;

    @Test
    public void test() {
        assertThat(config.getQueue()).isEqualTo("TEST");
        assertThat(config.getMemoryTimeout()).isEqualTo(15);
    }
}
