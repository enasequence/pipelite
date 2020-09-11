package pipelite.process.builder;

import lombok.Value;
import pipelite.executor.TaskExecutor;
import pipelite.executor.command.LocalCommandExecutor;
import pipelite.executor.command.LocalTaskExecutor;
import pipelite.executor.command.SshCommandExecutor;
import pipelite.executor.command.SshTaskExecutor;
import pipelite.executor.lsf.LsfLocalCommandExecutor;
import pipelite.executor.lsf.LsfLocalTaskExecutor;
import pipelite.executor.lsf.LsfSshCommandExecutor;
import pipelite.executor.lsf.LsfSshTaskExecutor;
import pipelite.task.TaskInstance;
import pipelite.task.TaskParameters;

import java.util.Optional;

@Value
public class TaskBuilder {
  private final ProcessBuilder processBuilder;
  private final String taskName;
  private final String dependsOnTaskName;
  private final TaskParameters taskParameters;

  public ProcessBuilderDependsOn executor(TaskExecutor executor) {
    return addTaskInstance(executor);
  }

  public ProcessBuilderDependsOn localCommandExecutor(String cmd) {
    return addTaskInstance(new LocalCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn localTaskExecutor(TaskExecutor executor) {
    return addTaskInstance(new LocalTaskExecutor(executor));
  }

  public ProcessBuilderDependsOn sshCommandExecutor(String cmd) {
    return addTaskInstance(new SshCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn sshTaskExecutor(TaskExecutor executor) {
    return addTaskInstance(new SshTaskExecutor(executor));
  }

  public ProcessBuilderDependsOn lsfLocalCommandExecutor(String cmd) {
    return addTaskInstance(new LsfLocalCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn lsfLocalTaskExecutor(TaskExecutor executor) {
    return addTaskInstance(new LsfLocalTaskExecutor(executor));
  }

  public ProcessBuilderDependsOn lsfSshCommandExecutor(String cmd) {
    return addTaskInstance(new LsfSshCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn lsfSshTaskExecutor(TaskExecutor executor) {
    return addTaskInstance(new LsfSshTaskExecutor(executor));
  }

  private ProcessBuilderDependsOn addTaskInstance(TaskExecutor executor) {
    TaskInstance dependsOn = null;
    if (dependsOnTaskName != null) {
      Optional<TaskInstance> dependsOnOptional =
          processBuilder.taskInstances.stream()
              .filter(taskInstance -> taskInstance.getTaskName().equals(dependsOnTaskName))
              .findFirst();

      if (!dependsOnOptional.isPresent()) {
        throw new IllegalArgumentException("Unknown task dependency: " + dependsOnTaskName);
      }
      dependsOn = dependsOnOptional.get();
    }

    processBuilder.taskInstances.add(
        TaskInstance.builder()
            .processName(processBuilder.processName)
            .processId(processBuilder.processId)
            .taskName(taskName)
            .executor(executor)
            .dependsOn(dependsOn)
            .taskParameters(taskParameters)
            .build());
    return new ProcessBuilderDependsOn(processBuilder);
  }
}
