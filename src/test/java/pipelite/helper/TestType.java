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
package pipelite.helper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum TestType {
  SUCCESS(0),
  NON_PERMANENT_ERROR(1),
  PERMANENT_ERROR(2);

  private final int exitCode;

  TestType(int exitCode) {
    this.exitCode = exitCode;
  }

  public int exitCode() {
    return exitCode;
  }

  public List<Integer> permanentErrors() {
    if (this == TestType.PERMANENT_ERROR) {
      return Arrays.asList(exitCode);
    }
    return Collections.emptyList();
  }

  public String cmd() {
    return "bash -c 'exit '" + exitCode;
  }

  public String image() {
    return "debian";
  }

  public List<String> imageArgs() {
    return Arrays.asList("bash", "-c", "exit " + exitCode);
  }
}
