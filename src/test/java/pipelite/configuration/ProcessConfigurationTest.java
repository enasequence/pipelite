package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class ProcessConfigurationTest {

    @Autowired
    ProcessConfiguration config;

    @Test
    public void test() {
        assertThat(config.getResolver()).isEqualTo("pipelite.task.result.resolver.TaskExecutionResultResolver#DEFAULT_EXCEPTION_RESOLVER");
    }
}
