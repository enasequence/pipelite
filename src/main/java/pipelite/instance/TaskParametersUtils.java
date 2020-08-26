package pipelite.instance;

import com.google.common.base.Supplier;
import lombok.Value;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Value
public class TaskParametersUtils {

  public static Integer getMemory(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return getValue(taskParameters::getMemory, defaultParameters::getMemory);
  }

  public static Integer getMemoryTimeout(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return getValue(taskParameters::getMemoryTimeout, defaultParameters::getMemoryTimeout);
  }

  public static Integer getCores(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return getValue(taskParameters::getCores, defaultParameters::getCores);
  }

  public static String getQueue(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return getValue(taskParameters::getQueue, defaultParameters::getQueue);
  }

  public static Integer getRetries(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return getValue(taskParameters::getRetries, defaultParameters::getRetries);
  }

  public static String getTempDir(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return getValue(taskParameters::getTempDir, defaultParameters::getTempDir);
  }

  public static String[] getEnv(TaskParameters taskParameters, TaskParameters defaultParameters) {
    return mergeEnv(taskParameters.getEnv(), defaultParameters.getEnv());
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
