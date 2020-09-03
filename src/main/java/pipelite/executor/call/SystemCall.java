package pipelite.executor.call;

import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import pipelite.executor.stream.KeepOldestByteArrayOutputStream;
import org.apache.commons.text.StringTokenizer;
import org.apache.commons.text.matcher.StringMatcher;
import org.apache.commons.text.matcher.StringMatcherFactory;
import pipelite.task.TaskParameters;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static pipelite.task.TaskExecutionResultExitCodeSerializer.EXIT_CODE_ERROR;

@Flogger
public class SystemCall implements CallExecutor.Call {

  @Override
  public CallExecutor.CallResult call(String cmd, TaskParameters taskParameters) {
    if (cmd == null) {
      return new CallExecutor.CallResult(EXIT_CODE_ERROR, null, null);
    }

    StringTokenizer st = new StringTokenizer(cmd);
    StringMatcher sm = StringMatcherFactory.INSTANCE.quoteMatcher();
    st.setQuoteMatcher(sm);
    List<String> args = st.getTokenList();
    if (args.isEmpty()) {
      return new CallExecutor.CallResult(EXIT_CODE_ERROR, null, null);
    }

    try {
      CommandLine commandLine = new CommandLine(args.get(0));
      if (args.size() > 1) {
        commandLine.addArguments(args.subList(1, args.size()).toArray(new String[0]));
      }
      OutputStream stdoutStream = new KeepOldestByteArrayOutputStream();
      OutputStream stderrStream = new KeepOldestByteArrayOutputStream();

      Executor executor = new DefaultExecutor();

      executor.setExitValues(null);

      executor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));
      // executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      executor.setWatchdog(
          new ExecuteWatchdog(
              taskParameters.getTimeout() != null
                  ? taskParameters.getTimeout().toMillis()
                  : ExecuteWatchdog.INFINITE_TIMEOUT));

      log.atInfo().log("Executing system call: %s" + cmd);

      int exitCode = executor.execute(commandLine, taskParameters.getEnvAsMap());
      return new CallExecutor.CallResult(
          exitCode, getStream(stdoutStream), getStream(stderrStream));

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed system call: %s", cmd);
      return new CallExecutor.CallResult(EXIT_CODE_ERROR, null, null);
    }
  }

  private String getStream(OutputStream stdoutStream) {
    try {
      stdoutStream.flush();
      String value = stdoutStream.toString();
      log.atInfo().log(value);
      stdoutStream.close();
      return value;
    } catch (IOException e) {
      return null;
    } finally {
    }
  }
}
