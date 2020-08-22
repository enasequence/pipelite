package pipelite;

import java.util.Random;

public class RandomStringGenerator {

  private static final Random random = new Random();

  public static String randomLauncherName() {
    return random(10);
  }

  public static String randomProcessName() {
    return random(10);
  }

  public static String randomProcessId() {
    return random(10);
  }

  public static String randomStageName() {
    return random(10);
  }

  private static String random(int length) {
    int leftLimit = 48; // numeral '0'
    int rightLimit = 122; // letter 'z'

    return random
        .ints(leftLimit, rightLimit + 1)
        .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }
}
