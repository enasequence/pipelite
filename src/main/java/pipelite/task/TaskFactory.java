package pipelite.task;

public interface TaskFactory<T extends Task> {
  T createTask();
}
