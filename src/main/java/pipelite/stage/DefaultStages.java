package pipelite.stage;

public enum DefaultStages implements Stage {
  STAGE_1(Task.class, null, 1, 1),
  STAGE_2(Task.class, STAGE_1, 1, 1);

  public static class Task implements pipelite.task.Task {

    @Override
    public void run() {
      System.out.println("Test");
    }
  }

  DefaultStages(Class<? extends Task> taskClass, DefaultStages dependsOn, int memory, int cores) {
    this.taskClass = taskClass;
    this.dependsOn = dependsOn;
    this.memory = memory;
    this.cores = cores;
  }

  private final Class<? extends Task> taskClass;
  private final DefaultStages dependsOn;
  private final int memory;
  private final int cores;

  @Override
  public Class<? extends Task> getTaskClass() {
    return taskClass;
  }

  public DefaultStages getDependsOn() {
    return dependsOn;
  }

  @Override
  public int getMemory() {
    return memory;
  }

  @Override
  public int getCores() {
    return cores;
  }
}
