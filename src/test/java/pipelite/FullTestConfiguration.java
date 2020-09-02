package pipelite;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskConfiguration;

@EnableAutoConfiguration
@EnableConfigurationProperties(
    value = {LauncherConfiguration.class, ProcessConfiguration.class, TaskConfiguration.class})
@ComponentScan(
    basePackages = {
      "pipelite.service",
      "pipelite.launcher",
      "pipelite.repository",
      "pipelite.configuration"
    })
public class FullTestConfiguration {}
