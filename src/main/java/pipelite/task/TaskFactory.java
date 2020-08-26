package pipelite.task;

public interface TaskFactory {
  Task createTask(TaskInfo taskInfo);
}
