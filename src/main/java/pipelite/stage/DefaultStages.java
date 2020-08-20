package pipelite.stage;

import pipelite.configuration.TaskConfiguration;

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
    taskConfiguration = new TaskConfiguration();
    taskConfiguration.setMemory(memory);
    taskConfiguration.setCores(cores);
  }

  private final Class<? extends Task> taskClass;
  private final DefaultStages dependsOn;
  private final TaskConfiguration taskConfiguration;

  @Override
  public String getStageName() {
    return this.name();
  }

  @Override
  public Class<? extends Task> getTaskClass() {
    return taskClass;
  }

  @Override
  public DefaultStages getDependsOn() {
    return dependsOn;
  }

  @Override
  public TaskConfiguration getTaskConfiguration() {
    return taskConfiguration;
  }
}
