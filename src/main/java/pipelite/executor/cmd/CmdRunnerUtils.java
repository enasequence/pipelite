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

import com.google.common.io.CharStreams;
import pipelite.exception.PipeliteException;

import static org.apache.commons.exec.util.StringUtils.isQuoted;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

public class CmdRunnerUtils {

  public static String quoteArguments(List<String> arguments) {
    return arguments.stream().map(arg -> quoteArgument(arg)).collect(Collectors.joining(" "));
  }

  public static String quoteArgument(String argument) {
    if (argument.startsWith("-") || isQuoted(argument)) {
      return argument;
    }
    return "'" + argument.replaceAll("'", "") + "'";
  }

  public static String read(URL url) {
    try {
      return read(url.openStream());
    } catch (IOException ex) {
      throw new PipeliteException(ex);
    }
  }

  public static String read(InputStream strm) {
    try (Reader reader = new InputStreamReader(strm)) {
      return CharStreams.toString(reader);
    } catch (IOException ex) {
      throw new PipeliteException(ex);
    }
  }

  public static void write(String str, OutputStream out) throws IOException {
    out.write(str.getBytes());
    out.flush();
  }

  public static void write(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[8192];
    int length;
    while ((length = in.read(buf)) > 0) {
      out.write(buf, 0, length);
    }
  }
}
