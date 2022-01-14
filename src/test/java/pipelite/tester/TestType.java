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
package pipelite.tester;

import static pipelite.tester.TestTypeConfiguration.*;

import java.util.function.Function;

public enum TestType {
  SUCCESS(
      execCount -> {
        if (execCount == 0) {
          return EXIT_CODE_SUCCESS;
        } else {
          return null;
        }
      }),
  NON_PERMANENT_ERROR(execCount -> EXIT_CODE_NON_PERMANENT_ERROR),
  SUCCESS_AFTER_ONE_NON_PERMANENT_ERROR(
      execCount -> {
        if (execCount == 0) {
          return EXIT_CODE_NON_PERMANENT_ERROR;
        } else if (execCount == 1) {
          return EXIT_CODE_SUCCESS;
        } else {
          return null;
        }
      }),
  PERMANENT_ERROR(
      execCount -> {
        if (execCount == 0) {
          return EXIT_CODE_PERMANENT_ERROR;
        } else {
          return null;
        }
      });

  private final Function<Integer, Integer> exitCodeResolver;

  TestType(Function<Integer, Integer> exitCodeResolver) {
    this.exitCodeResolver = exitCodeResolver;
  }

  public String exitCode(int execCount) {
    Integer exitCode = exitCodeResolver.apply(execCount);
    if (exitCode == null) {
      return "";
    }
    return String.valueOf(exitCode);
  }
}
