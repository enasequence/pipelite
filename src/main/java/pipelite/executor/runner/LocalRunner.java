/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.runner;

import static pipelite.task.TaskExecutionResultExitCode.EXIT_CODE_ERROR;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import org.apache.commons.text.StringTokenizer;
import org.apache.commons.text.matcher.StringMatcher;
import org.apache.commons.text.matcher.StringMatcherFactory;
import pipelite.executor.stream.KeepOldestByteArrayOutputStream;
import pipelite.task.TaskParameters;

@Flogger
public class LocalRunner implements CommandRunner {

  @Override
  public CommandRunnerResult execute(String cmd, TaskParameters taskParameters) {
    if (cmd == null) {
      return new CommandRunnerResult(EXIT_CODE_ERROR, null, null);
    }

    StringTokenizer st = new StringTokenizer(cmd);
    StringMatcher sm = StringMatcherFactory.INSTANCE.quoteMatcher();
    st.setQuoteMatcher(sm);
    List<String> args = st.getTokenList();
    if (args.isEmpty()) {
      return new CommandRunnerResult(EXIT_CODE_ERROR, null, null);
    }

    try {
      CommandLine commandLine = new CommandLine(args.get(0));
      if (args.size() > 1) {
        commandLine.addArguments(args.subList(1, args.size()).toArray(new String[0]), false);
      }
      OutputStream stdoutStream = new KeepOldestByteArrayOutputStream();
      OutputStream stderrStream = new KeepOldestByteArrayOutputStream();

      Executor apacheExecutor = new DefaultExecutor();

      apacheExecutor.setExitValues(null);

      apacheExecutor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));
      // executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      Duration timeout = taskParameters.getTimeout();
      if (timeout != null) {
        apacheExecutor.setWatchdog(
            new ExecuteWatchdog(
                taskParameters.getTimeout() != null
                    ? taskParameters.getTimeout().toMillis()
                    : ExecuteWatchdog.INFINITE_TIMEOUT));
      }

      log.atInfo().log("Executing system call: %s", cmd);

      int exitCode = apacheExecutor.execute(commandLine, taskParameters.getEnvAsMap());
      return new CommandRunnerResult(exitCode, getStream(stdoutStream), getStream(stderrStream));

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed system call: %s", cmd);
      return new CommandRunnerResult(EXIT_CODE_ERROR, null, null);
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
