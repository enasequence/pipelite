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
package pipelite.executor.cmd;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.List;
import lombok.extern.flogger.Flogger;
import org.apache.commons.exec.*;
import org.apache.commons.text.StringTokenizer;
import org.apache.commons.text.matcher.StringMatcher;
import org.apache.commons.text.matcher.StringMatcherFactory;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.CmdExecutorParameters;

@Flogger
public class LocalCmdRunner implements CmdRunner {

  private final CmdExecutorParameters executorParams;

  public LocalCmdRunner(CmdExecutorParameters executorParams) {
    this.executorParams = executorParams;
  }

  @Override
  public StageExecutorResult execute(String cmd) {
    if (cmd == null) {
      throw new PipeliteException("No command to execute");
    }

    StringTokenizer st = new StringTokenizer(cmd);
    StringMatcher sm = StringMatcherFactory.INSTANCE.quoteMatcher();
    st.setQuoteMatcher(sm);
    List<String> args = st.getTokenList();
    if (args.isEmpty()) {
      throw new PipeliteException("No command to execute");
    }

    try {
      CommandLine commandLine = new CommandLine(args.get(0));
      if (args.size() > 1) {
        commandLine.addArguments(args.subList(1, args.size()).toArray(new String[0]), false);
      }

      OutputStream stdoutStream = new ByteArrayOutputStream();
      OutputStream stderrStream = new ByteArrayOutputStream();

      Executor apacheExecutor = new DefaultExecutor();

      apacheExecutor.setExitValues(null);

      apacheExecutor.setStreamHandler(new PumpStreamHandler(stdoutStream, stderrStream));
      // executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));

      Duration timeout = executorParams.getTimeout();
      if (timeout != null) {
        apacheExecutor.setWatchdog(
            new ExecuteWatchdog(
                executorParams.getTimeout() != null
                    ? executorParams.getTimeout().toMillis()
                    : ExecuteWatchdog.INFINITE_TIMEOUT));
      }

      log.atInfo().log("Executing local call: %s", cmd);

      int exitCode = apacheExecutor.execute(commandLine, executorParams.getEnv());
      return CmdRunner.result(cmd, exitCode, getStream(stdoutStream), getStream(stderrStream));

    } catch (Exception ex) {
      throw new PipeliteException("Failed to execute local call: " + cmd, ex);
    }
  }

  @Override
  public void writeFile(String str, Path path) {
    log.atInfo().log("Writing file %s", path);
    try {
      CmdRunnerUtils.write(str, new FileOutputStream(path.toFile()));
    } catch (IOException ex) {
      throw new PipeliteException("Failed to write file " + path, ex);
    }
  }

  @Override
  public void deleteFile(Path path) {
    log.atInfo().log("Deleting file %s", path);
    try {
      Files.delete(path);
    } catch (IOException ex) {
      throw new PipeliteException("Failed to delete file " + path, ex);
    }
  }

  private String getStream(OutputStream stdoutStream) {
    try {
      stdoutStream.flush();
      String value = stdoutStream.toString();
      // log.atInfo().log(value);
      stdoutStream.close();
      return value;
    } catch (IOException e) {
      return null;
    }
  }
}
