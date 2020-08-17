package pipelite;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pipelite.configuration.LSFTaskExecutorConfiguration;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.configuration.TaskExecutorConfiguration;

// TODO: this class can be removed once other classes are creating using Spring and their configurations can be autowired

@Component
public class ApplicationConfiguration {

    @Autowired
    public LauncherConfiguration launcherConfiguration;

    @Autowired
    public ProcessConfiguration processConfiguration;

    @Autowired
    public TaskExecutorConfiguration taskExecutorConfiguration;

    @Autowired
    public LSFTaskExecutorConfiguration lsfTaskExecutorConfiguration;
}
