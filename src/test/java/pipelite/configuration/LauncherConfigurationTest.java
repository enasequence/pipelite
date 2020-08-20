package pipelite.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.TestConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TestConfiguration.class)
@ActiveProfiles("test")
public class LauncherConfigurationTest {

    @Autowired
    LauncherConfiguration config;

    @Test
    public void test() {
        assertThat(config.getLauncherName()).isEqualTo("TEST");
        assertThat(config.getWorkers()).isEqualTo(1);
    }
}
