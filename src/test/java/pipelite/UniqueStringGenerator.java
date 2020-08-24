package pipelite;

import java.util.UUID;

public class UniqueStringGenerator {

  public static String randomLauncherName() {
    return id();
  }

  public static String randomProcessName() {
    return id();
  }

  public static String randomProcessId() {
    return id();
  }

  public static String randomStageName() {
    return id();
  }

  private static String id() {
    return UUID.randomUUID().toString();
  }
}
