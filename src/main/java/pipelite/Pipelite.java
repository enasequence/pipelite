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

import picocli.CommandLine;

@CommandLine.Command(mixinStandardHelpOptions = true)
public class Pipelite {

  @CommandLine.ArgGroup(multiplicity = "1")
  Mode mode;

  public static class Mode {
    @CommandLine.Option(
        names = {"-l", "-launcher"},
        description = "Run the pipelite launcher")
    public boolean launcher;

    @CommandLine.Option(
        names = {"-s", "-scheduler"},
        description = "Run the pipelite scheduler")
    public boolean scheduler;
  }

  @CommandLine.Option(
      names = {"-u", "-unlock"},
      description = "Remove all launcher or scheduler locks")
  public boolean removeLocks;

  public static int run(String[] args) {
    try {
      return _run(args, true);
    } catch (Exception ex) {
      ex.printStackTrace();
      return 1;
    }
  }

  public static int _run(String[] args, boolean run) {
    Pipelite options = new Pipelite();

    CommandLine commandLine;
    CommandLine.ParseResult parseResult;

    try {
      commandLine = new CommandLine(options);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      return 1;
    }

    try {
      parseResult = commandLine.parseArgs(args);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      commandLine.usage(System.out);
      return 1;
    }

    if (parseResult.isUsageHelpRequested()) {
      commandLine.usage(System.out);
      return 1;
    }

    if (run) {
      if (options.mode.launcher) {
        new PipeliteLauncherRunner().run(options);
      } else {
        new PipeliteSchedulerRunner().run(options);
      }
    }
    return 0;
  }
}
