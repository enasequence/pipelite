/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.mock.schedule;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

/*
 * SAMPLE CommitStatus. Must be substituted
 */
public enum ExecutionResults implements ExecutionResult {
  OK(null, 0, null, RESULT_TYPE.SUCCESS),
  SKIPPED("SKIPPED", 51, Exception.class, RESULT_TYPE.SUCCESS),
  UNKNOWN_FAILURE("UNKNOWN_FAILURE", 53, Throwable.class, RESULT_TYPE.TRANSIENT_ERROR);

  ExecutionResults(String message, int exit_code, Class<?> cause, RESULT_TYPE type) {
    this.message = message;
    this.type = type;
    this.exit_code = (byte) exit_code;
    this.cause = cause;
  }

  final String message;
  final RESULT_TYPE type;
  final byte exit_code;
  final Class<?> cause;

  @Override
  public byte getExitCode() {
    return exit_code;
  }

  @Override
  public Class<Throwable> getCause() {
    return (Class<Throwable>) cause;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public RESULT_TYPE getType() {
    return type;
  }
}
