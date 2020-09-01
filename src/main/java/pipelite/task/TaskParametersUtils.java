package pipelite.task;

import com.google.common.base.Supplier;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Value
public class TaskParametersUtils {

  public static String getHost(
          TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getHost, taskConfiguration::getHost);
  }

  public static Duration getTimeout(
          TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getTimeout, taskConfiguration::getTimeout);
  }

  public static Integer getRetries(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getRetries, taskConfiguration::getRetries);
  }

  public static String getTempDir(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getTempDir, taskConfiguration::getTempDir);
  }

  public static String[] getEnv(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return mergeEnv(taskParameters.getEnv(), taskConfiguration.getEnv());
  }

  public static Integer getMemory(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getMemory, taskConfiguration::getMemory);
  }

  public static Duration getMemoryTimeout(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getMemoryTimeout, taskConfiguration::getMemoryTimeout);
  }

  public static Integer getCores(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getCores, taskConfiguration::getCores);
  }

  public static String getQueue(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getQueue, taskConfiguration::getQueue);
  }

  private static <T> T getValue(Supplier<T> taskInstance, Supplier<T> taskConfiguration) {
    T value = taskInstance.get();
    if (value == null) {
      value = taskConfiguration.get();
    }
    return value;
  }

  private static String[] mergeEnv(String[] taskInstance, String[] taskConfiguration) {
    if (taskInstance == null) {
      taskInstance = new String[0];
    }
    if (taskConfiguration == null) {
      taskConfiguration = new String[0];
    }
    Set<String> set1 = new HashSet<>(Arrays.asList(taskInstance));
    Set<String> set2 = new HashSet<>(Arrays.asList(taskConfiguration));
    set1.addAll(set2);
    return set1.toArray(new String[0]);
  }
}
