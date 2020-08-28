package pipelite;

import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import pipelite.instance.TaskInstance;
import pipelite.instance.TaskParameters;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteLauncherServiceManager;
import pipelite.resolver.TaskExecutionResultResolver;
import pipelite.task.TaskExecutionResultExitCodeSerializer;
import uk.ac.ebi.ena.sra.pipeline.launcher.InternalTaskExecutor;

@Flogger
@SpringBootApplication
public class Application implements CommandLineRunner {

  @Autowired PipeliteLauncher pipeliteLauncher;

  public static final String TASK_MODE = "task";

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Override
  public void run(String... args) {
    try {
      if (args.length == 4 && TASK_MODE.equals(args[0])) {
        System.exit(task());
      } else {
        launcher();
      }
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Uncaught exception");
      throw ex;
    }
  }

  private void launcher() {
    PipeliteLauncherServiceManager.run(pipeliteLauncher);
  }

  private int task(String... args) {
    String processName = args[1];
    String processId = args[2];
    String taskName = args[3];
    String resolverName = args[4];

    TaskExecutionResultResolver resolver = null;
    try {
      resolver = (TaskExecutionResultResolver) Class.forName(resolverName).newInstance();
    } catch (Exception ex) {
      System.exit(TaskExecutionResultExitCodeSerializer.INTERNAL_ERROR_EXIT_CODE);
    }

    TaskParameters taskParameters = TaskParameters.builder().build();

    InternalTaskExecutor internalTaskExecutor = new InternalTaskExecutor();

    // Task specific configuration is not available when a task is being executed using internal
    // task executor.

    TaskInstance taskInstance =
        TaskInstance.builder()
            .processName(processName)
            .processId(processId)
            .taskName(taskName)
            // Executor is InternalTaskExecutor
            // .executor()
            .resolver(resolver)
            .taskParameters(taskParameters)
            .build();

    return taskInstance
        .getResolver()
        .serializer()
        .serialize(internalTaskExecutor.execute(taskInstance));
  }
}
