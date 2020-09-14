package pipelite.process.builder;

import pipelite.executor.TaskExecutor;
import pipelite.executor.command.LocalCommandExecutor;
import pipelite.executor.command.LocalTaskExecutor;
import pipelite.executor.command.SshCommandExecutor;
import pipelite.executor.command.SshTaskExecutor;
import pipelite.executor.lsf.LsfLocalCommandExecutor;
import pipelite.executor.lsf.LsfLocalTaskExecutor;
import pipelite.executor.lsf.LsfSshCommandExecutor;
import pipelite.executor.lsf.LsfSshTaskExecutor;
import pipelite.task.Task;
import pipelite.task.TaskParameters;

import java.util.Optional;

public class TaskBuilder {
  private final ProcessBuilder processBuilder;
  private final String taskName;
  private final String dependsOnTaskName;
  private final TaskParameters taskParameters;

  public TaskBuilder(ProcessBuilder processBuilder, String taskName, String dependsOnTaskName, TaskParameters taskParameters) {
    this.processBuilder = processBuilder;
    this.taskName = taskName;
    this.dependsOnTaskName = dependsOnTaskName;
    this.taskParameters = taskParameters;
  }

  public ProcessBuilderDependsOn executor(TaskExecutor executor) {
    return addTask(executor);
  }

  public ProcessBuilderDependsOn localCommandExecutor(String cmd) {
    return addTask(new LocalCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn localTaskExecutor(TaskExecutor executor) {
    return addTask(new LocalTaskExecutor(executor));
  }

  public ProcessBuilderDependsOn sshCommandExecutor(String cmd) {
    return addTask(new SshCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn sshTaskExecutor(TaskExecutor executor) {
    return addTask(new SshTaskExecutor(executor));
  }

  public ProcessBuilderDependsOn lsfLocalCommandExecutor(String cmd) {
    return addTask(new LsfLocalCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn lsfLocalTaskExecutor(TaskExecutor executor) {
    return addTask(new LsfLocalTaskExecutor(executor));
  }

  public ProcessBuilderDependsOn lsfSshCommandExecutor(String cmd) {
    return addTask(new LsfSshCommandExecutor(cmd));
  }

  public ProcessBuilderDependsOn lsfSshTaskExecutor(TaskExecutor executor) {
    return addTask(new LsfSshTaskExecutor(executor));
  }

  private ProcessBuilderDependsOn addTask(TaskExecutor executor) {
    Task dependsOn = null;
    if (dependsOnTaskName != null) {
      Optional<Task> dependsOnOptional =
          processBuilder.tasks.stream()
              .filter(taskInstance -> taskInstance.getTaskName().equals(dependsOnTaskName))
              .findFirst();

      if (!dependsOnOptional.isPresent()) {
        throw new IllegalArgumentException("Unknown task dependency: " + dependsOnTaskName);
      }
      dependsOn = dependsOnOptional.get();
    }

    processBuilder.tasks.add(
        Task.builder()
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
