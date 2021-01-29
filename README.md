Pipelite workflow manager
=======

### Overview

Pipelite is a workflow manager that executes pipelines or schedules. Pipelines are used for parallel executions of
processes. Schedules are used for executing processes according to a cron schedule. Only one scheduled process can be
active for a pipeline at any given time.

Pipeline starts up a web service with a basic monitoring interface, takes care of the pipeline and schedule executions
and stores the execution state in a relational database.

### How to start up Pipelite

Pipelite is a Spring java application distributed as a regular maven jar archive. The jar should be added as a
dependency to a application that registers pipelines and schedules to be executed by Pipelite.

A minimal build.gradle could contain:

```gradle
plugins {
id 'org.springframework.boot' version '2.4.0'
id 'io.spring.dependency-management' version '1.0.10.RELEASE'
}

dependencies {
implementation 'org.springframework.boot:spring-boot-starter'
implementation 'pipelite:pipelite:1.0.89'
}
```

and the minimal application would call  ```Pipelite.main``` to start up the Pipelite services:

```java

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        Pipelite.main(args);
    }
}
```

### How to configure Pipelite schedules

Schedules are used for executing processes according to a cron schedule.

Schedules are registered by implementing the ```Pipelite.Schedule``` interface and using the ```@Component```
annotation.

#### SimpleLsfExecutor example

A schedule executed using ```SimpleLsfExecutor``` over ssh would look like the following:

```java

@Component
public class MySchedule implements Pipelite.Schedule {

    @Override
    public String pipelineName() {
        // A unique name for the schedule.  
        return "MySchedule";
    }

    // The command line command to execute in STAGE1.
    private static final String STAGE_CMD1 = "...";

    // The command line command to execute in STAGE2.
    private static final String STAGE_CMD2 = "...";

    @Override
    public Options configurePipeline() {
        // The cron expression for the schedule.
        return new Options().cron("* * * * *");
    }

    @Override
    public void configureProcess(ProcessBuilder builder) {
        // A process with two stages is configured here. As the stages do not 
        // depend on each other they will be executed in parallel.
        builder
                // Execute STAGE1 stage that does not depend on any other stage.
                // Different executeXXX methods exist for different types of stage dependencies.
                .execute("STAGE1")
                // Execute STAGE1 using SimpleLsfExecutor with the provided stage execution parameters.
                // Different withXXX methods exist for different execution backends.
                .withSimpleLsfExecutor(STAGE_CMD1, STAGE_PARAMS)
                // Execute STAGE2 stage that does not depend on any other stage.
                // Different executeXXX methods exist for different types of stage dependencies.
                .execute("STAGE2")
                // Execute STAGE2 using SimpleLsfExecutor with the provided stage execution parameters.
                // Different withXXX methods exist for different execution backends.
                .withSimpleLsfExecutor(STAGE_CMD2, STAGE_PARAMS);
    }

    // SimpleLsfExecutor stage execution parameters are defined here. They can be shared
    // between stages or each stage can be configured with different parameters.
    private static final SimpleLsfExecutorParameters STAGE_PARAMS =
            SimpleLsfExecutorParameters.builder()
                    // How may times stages are immediately retried if their execution fails.
                    .immediateRetries(2)
                    // The maximum number of times a stage is retried if its execution fails.
                    // Applies only to pipelines. As opposed to schedules, stages in pipelines 
                    // are temporary stopped after immediate retries and picked up later and 
                    // retried until maximum retries is exceeded.
                    .maximumRetries(5)
                    // The number of CPU cores required to execute the stage.
                    .cpu(1)
                    // The amount of memory in MBytes required to execute the stage.
                    .memory(16 /* MBytes */)
                    // The timeout after which the stage execution will be considered as failed.
                    .timeout(Duration.ofMinutes(30))
                    // The LSF login node.
                    .host("noah-login")
                    // The LSF queue.
                    .queue("production-rh74")
                    // The LSF working directoy where stage specific output files are written.
                    .workDir("pipelite_tmp")
                    .build();
}
```

### Pipelite configuration parameters

#### Service parameters

- pipelite.service.name: the name of the pipeline service. Only one service with the same name can be active at one
  time. Default value: hostname:port
- pipelite.service.port: the http port for the pipeline web interface. Default value: 8083
- pipelite.service.contextPath: the http port for the pipeline web interface. Default value: /pipelite
- pipelite.service.username: the pipelite web service username. Default value: pipelite
- pipelite.service.password: the pipelite web service password. Default value: pipelite
- pipelite.service.force: forces the pipelite service to start by removing all service locks and by updating service
  names attached to schedules if necessary. Default value: false
- pipelite.mail.

#### Mail parameters

- pipelite.mail.host: the SMTP host
- pipelite.mail.port: the SMTP port
- pipelite.mail.from: the email sender
- pipelite.mail.to: the email recipient(s)
- pipelite.mail.starttls: is starttls enabled. Default value: false
- pipelite.mail.username: the SMTP username for authentication purposes (optional)
- pipelite.mail.password: the SMTP username for authentication purposes (optional)
- pipelite.mail.processFailed: send an email when process fails. Default: true
- pipelite.mail.processCompleted: send an email when process completes. Default: false
- pipelite.mail.stageError: send an email when stage fails. Default: true
- pipelite.mail.stageSuccess: send an email when stage succeeds. Default: false

#### Database parameters

- pipelite.datasource.driverClassName: JDBC driver class name
- pipelite.datasource.url: JDBC URL
- pipelite.datasource.username: JDBC username
- pipelite.datasource.password: JDBC password
- pipelite.datasource.ddlAuto: see Hibernate hbm2ddl.auto options
- pipelite.datasource.dialect: see Hibernate dialect options
- pipelite.datasource.maxActive: maximum number of active database connections. Default value: 25
- pipelite.datasource.test: if set to true then uses an in memory database unsuitable for production purposes

#### Advanced parameters

public static final Duration DEFAULT_LOCK_FREQUENCY = Duration.ofMinutes(5); public static final Duration
DEFAULT_LOCK_DURATION = Duration.ofMinutes(60); private static final Duration DEFAULT_PROCESS_RUNNER_FREQUENCY =
Duration.ofSeconds(10); private static final Duration DEFAULT_SCHEDULE_REFRESH_FREQUENCY = Duration.ofHours(4); private
static final Duration DEFAULT_PROCESS_QUEUE_MAX_REFRESH_FREQUENCY = Duration.ofHours(6); private static final Duration
DEFAULT_PROCESS_QUEUE_MIN_REFRESH_FREQUENCY = Duration.ofMinutes(5); private static final int
DEFAULT_PROCESS_QUEUE_MAX_SIZE = 5000; private static final int DEFAULT_PROCESS_CREATE_MAX_SIZE = 1000;

public AdvancedConfiguration() {}

- pipelite.advanced.lockFrequency: the frequency of renewing service locks. Default value: 5 minutes.
- pipelite.advanced.lockDuration: the duration after which service and process locks expire unless the service lock is
  renewed. Default value: 60 minutes.
- pipelite.advanced.processRunnerFrequency: the running frequency for executing new processes. Default value: 10
  seconds.
- pipelite.advanced.processQueueMaxRefreshFrequency: the maximum frequency for pipeline execution queue to be refreshed
  to allow process re-prioritisation. Default value: 6 hours.
- pipelite.advanced.processQueueMinRefreshFrequency: the minimum frequency for pipeline execution queue to be refreshed
  to allow process re-prioritisation. Default value: 5 minutes.
- pipelite.advanced.processQueueMaxSize: The maximum size of pipeline execution queue. Default value: 5000
- pipelite.advanced.processCreateMaxSize: The maximum number of processes created from a process source at one time.
  Default value: 1000.

#### Test profiles

- if Spring active profiles contain 'test' then uses an in memory database unsuitable for production purposes
- if Spring active profiles contain 'test-lorem' then generates test content for the web interface
