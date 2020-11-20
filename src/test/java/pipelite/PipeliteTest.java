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
package pipelite;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;

public class PipeliteTest {

  @Test
  public void parseMissingRequiredOption() {
    try {
      ByteArrayOutputStream stdoutStream = getStdOutAsStream();
      ByteArrayOutputStream stderrStream = getStdErrAsStream();

      assertThat(Pipelite.parse(new String[] {})).isNull();

      assertThat(stdoutStream.toString())
          .startsWith("Error: Missing required argument (specify one of these): (-l | -s)");
      assertThat(stderrStream.toString()).isEqualTo("");

    } finally {
      System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
      System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
    }
  }

  @Test
  public void parseMutuallyExclusiveOption() {
    try {
      ByteArrayOutputStream stdoutStream = getStdOutAsStream();
      ByteArrayOutputStream stderrStream = getStdErrAsStream();

      assertThat(Pipelite.parse(new String[] {"-l", "-s"})).isNull();

      assertThat(stdoutStream.toString())
          .startsWith("Error: -launcher, -scheduler are mutually exclusive");
      assertThat(stderrStream.toString()).isEqualTo("");

    } finally {
      System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
      System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
    }
  }

  @Test
  public void parseUnknownOption() {
    try {
      ByteArrayOutputStream stdoutStream = getStdOutAsStream();
      ByteArrayOutputStream stderrStream = getStdErrAsStream();

      assertThat(Pipelite.parse(new String[] {"-a"})).isNull();

      assertThat(stdoutStream.toString()).startsWith("Unknown option: '-a'");
      assertThat(stderrStream.toString()).isEqualTo("");

    } finally {
      System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
      System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
    }
  }

  @Test
  public void parseLauncherOption() {
    try {
      ByteArrayOutputStream stdoutStream = getStdOutAsStream();
      ByteArrayOutputStream stderrStream = getStdErrAsStream();

      PipeliteOptions options = Pipelite.parse(new String[] {"-l"});
      assertThat(options).isNotNull();

      assertThat(stdoutStream.toString()).startsWith("");
      assertThat(stderrStream.toString()).isEqualTo("");

    } finally {
      System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
      System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
    }
  }

  @Test
  public void parseSchedulerOption() {
    try {
      ByteArrayOutputStream stdoutStream = getStdOutAsStream();
      ByteArrayOutputStream stderrStream = getStdErrAsStream();

      PipeliteOptions options = Pipelite.parse(new String[] {"-s"});
      assertThat(options).isNotNull();

      assertThat(stdoutStream.toString()).startsWith("");
      assertThat(stderrStream.toString()).isEqualTo("");

    } finally {
      System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
      System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
    }
  }

  private static ByteArrayOutputStream getStdOutAsStream() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    System.setOut(ps);
    return baos;
  }

  private static ByteArrayOutputStream getStdErrAsStream() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    System.setErr(ps);
    return baos;
  }
}
