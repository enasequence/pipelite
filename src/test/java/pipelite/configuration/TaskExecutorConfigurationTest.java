package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class TaskExecutorConfigurationTest {

    @Autowired
    TaskExecutorConfiguration config;

    @Test
    public void test() {
        assertThat(config.getMemory()).isEqualTo(1);
        assertThat(config.getCores()).isEqualTo(1);
        assertThat(config.getRetries()).isEqualTo(3);
        assertThat(config.getTempDir()).isBlank();
    }
}
