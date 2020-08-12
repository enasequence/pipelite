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
package uk.ac.ebi.ena.sra.pipeline.launcher.iface;

public interface ExecutionResult {
  public enum RESULT_TYPE {
    SUCCESS(false, false),
    SKIPPED(false, false),
    TRANSIENT_ERROR(true, true), /* consider ERROR */
    PERMANENT_ERROR(true, false); /* consider FATAL */

    RESULT_TYPE(boolean is_failure, boolean can_reprocess) {
      this.is_failure = is_failure;
      this.can_reprocess = can_reprocess;
    }

    final boolean is_failure;
    final boolean can_reprocess;

    public boolean isFailure() {
      return is_failure;
    }

    public boolean canReprocess() {
      return can_reprocess;
    }
  };

  public RESULT_TYPE getType();

  public byte getExitCode(); // exit code for status. unique.

  public Class<? extends Throwable> getCause(); // throwable that can cause the status. unique.

  public String getMessage(); // commit message. unique.
}
