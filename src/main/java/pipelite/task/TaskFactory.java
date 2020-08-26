package pipelite.task;

public interface TaskFactory {
  Task createTask(String stageName);
}
