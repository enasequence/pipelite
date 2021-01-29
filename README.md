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