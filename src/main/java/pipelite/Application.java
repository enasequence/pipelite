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

import lombok.extern.flogger.Flogger;
import picocli.CommandLine;

@CommandLine.Command(mixinStandardHelpOptions = true)
public class Application {

  @CommandLine.ArgGroup(multiplicity = "1")
  Mode mode;

  private static class Mode {
    @CommandLine.Option(
        names = {"-l", "-launcher"},
        description = "Run the pipelite launcher")
    boolean launcher;

    @CommandLine.Option(
        names = {"-s", "-scheduler"},
        description = "Run the pipelite scheduler")
    boolean scheduler;
  }

  public static void main(String[] args) {
    try {
      System.exit(_main(args, true));
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
  }

  public static int _main(String[] args, boolean run) {
    Application app = new Application();
    try {
      new CommandLine(app).parseArgs(args);
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
      return 1;
    }
    if (run) {
      if (app.mode.launcher) {
        new PipeliteLauncherRunner().run();
      } else {
        new PipeliteSchedulerRunner().run();
      }
    }
    return 0;
  }
}
