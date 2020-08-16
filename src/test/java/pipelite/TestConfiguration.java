package pipelite;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import pipelite.configuration.LSFTaskExecutorConfiguration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskExecutorConfiguration;

@EnableAutoConfiguration
@EnableConfigurationProperties(
    value = {
      LauncherConfiguration.class,
      ProcessConfiguration.class,
      TaskExecutorConfiguration.class,
      LSFTaskExecutorConfiguration.class
    })
public class TestConfiguration {}