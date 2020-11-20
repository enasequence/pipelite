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

@CommandLine.Command(name = "Pipelite", mixinStandardHelpOptions = true)
public class PipeliteOptions {

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
}
