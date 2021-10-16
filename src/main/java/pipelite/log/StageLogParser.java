package pipelite.log;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the last error ({{PIPELITE_ERROR: <message>}}) or Java stack trace from the stage log.
 * Java exceptions must have Exception or Error suffix.
 */
public class StageLogParser {

  private static final Pattern PIPELITE_ERROR_PATTERN =
      Pattern.compile("^.*\\{\\{\\s*PIPELITE_ERROR\\s*:(.+)\\}\\}", Pattern.CASE_INSENSITIVE);

  private static final Pattern JAVA_EXCEPTION_MESSAGE_PATTERN =
      Pattern.compile("(((?:\\w(?:\\$+|\\.|\\/)?)+)(?:Exception|Error))", Pattern.CASE_INSENSITIVE);
  private static final Pattern JAVA_EXCEPTION_FRAME_PATTERN = Pattern.compile("^\\tat");
  private static final Pattern JAVA_EXCEPTION_CAUSE_PATTERN = Pattern.compile("^Caused\\s+by:");
  private static final Pattern JAVA_EXCEPTION_MORE_PATTERN =
      Pattern.compile("^\\s*\\.\\.\\.\\s+\\d+\\s+more");

  private static int MAX_ERROR_LENGTH = 256;

  private Scanner scanner;
  private String line;
  private String prevLine;
  private String nextLine;

  private String error;
  private String trace;

  private boolean nextLine() {
    if (nextLine != null) {
      // Read line from nextLine buffer not to loose it.
      line = nextLine;
      nextLine = null;
      return true;
    }
    if (scanner.hasNextLine()) {
      prevLine = line;
      line = scanner.nextLine().replaceAll("\\r$", "");
      return true;
    }
    return false;
  }

  private void readLine(StringBuilder str, Pattern... patterns) {
    nextLine:
    while (nextLine()) {
      for (Pattern pattern : patterns) {
        if (pattern.matcher(line).find()) {
          str.append(line);
          str.append("\n");
          continue nextLine;
        }
      }
      // Save line into nextLine buffer not to loose it.
      nextLine = line;
      return;
    }
  }

  private boolean parseError() {
    Matcher matcher = PIPELITE_ERROR_PATTERN.matcher(line);
    if (matcher.find()) {
      String trimmedError = matcher.group(1).trim();
      if (!trimmedError.isEmpty()) {
        error =
            (trimmedError.length() > MAX_ERROR_LENGTH)
                ? trimmedError.substring(1, MAX_ERROR_LENGTH)
                : trimmedError;
        return true;
      }
    }
    return false;
  }

  private boolean parseJavaException() {
    StringBuilder str = new StringBuilder();
    if (!JAVA_EXCEPTION_FRAME_PATTERN.matcher(line).find()) {
      return false;
    }
    int messageMatcherIndex = 0;
    Matcher messageMatcher = JAVA_EXCEPTION_MESSAGE_PATTERN.matcher(prevLine);
    while (messageMatcher.find(messageMatcherIndex)) {
      String message = prevLine.substring(messageMatcher.end()).trim();
      if (!message.isEmpty() && !message.startsWith(":")) {
        messageMatcherIndex = messageMatcher.end();
      } else {
        // Append exception class.
        str.append(messageMatcher.group());
        // Append exception message.
        str.append(message);
        str.append("\n");
        // Append stack trace line.
        str.append(line);
        str.append("\n");
        break;
      }
    }
    if (str.length() == 0) {
      return false;
    }
    // Append stack trace lines.
    readLine(
        str,
        JAVA_EXCEPTION_FRAME_PATTERN,
        JAVA_EXCEPTION_CAUSE_PATTERN,
        JAVA_EXCEPTION_MORE_PATTERN);
    trace = str.toString();
    return true;
  }

  public StageLogParser(String stageLog) {
    if (stageLog == null || stageLog.isEmpty()) {
      return;
    }
    scanner = new Scanner(stageLog);
    scanner.useDelimiter("\\n");
    while (nextLine()) {
      if (parseError() || parseJavaException()) {
        // Continue after first parsing success.
      }
    }
    scanner.close();
  }

  public String getError() {
    if (error != null && !error.isEmpty()) {
      return error;
    }
    return null;
  }

  public String getTrace() {
    if (trace != null && !trace.isEmpty()) {
      return trace;
    }
    return null;
  }
}
