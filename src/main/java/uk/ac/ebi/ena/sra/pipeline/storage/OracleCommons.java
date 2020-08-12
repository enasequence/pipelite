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
package uk.ac.ebi.ena.sra.pipeline.storage;

public interface OracleCommons {
  static final String PIPELINE_COLUMN_NAME = "PIPELINE_NAME";
  static final String PROCESS_COLUMN_NAME = "PROCESS_ID";
  static final String STAGE_NAME_COLUMN_NAME = "STAGE_NAME";

  static final String ATTEMPT_COLUMN_NAME = "EXEC_CNT";
  static final String ENABLED_COLUMN_NAME = "ENABLED";

  static final String EXEC_ID_SEQUENCE = "PIPELITE_EXEC_ID_SEQ";
  static final String EXEC_ID_COLUMN_NAME = "EXEC_ID";
  static final String EXEC_START_COLUMN_NAME = "EXEC_START";
  static final String EXEC_DATE_COLUMN_NAME = "EXEC_DATE";
  static final String EXEC_RESULT_COLUMN_NAME = "EXEC_RESULT";
  static final String EXEC_RESULT_TYPE_COLUMN_NAME = "EXEC_RESULT_TYPE";
  static final String EXEC_STDOUT_COLUMN_NAME = "EXEC_STDOUT";
  static final String EXEC_STDERR_COLUMN_NAME = "EXEC_STDERR";
  static final String EXEC_CMDLINE_COLUMN_NAME = "EXEC_CMD_LINE";

  static final String PROCESS_PRIORITY_COLUMN_NAME = "PRIORITY";
  static final String PROCESS_STATE_COLUMN_NAME = "STATE";
  static final String PROCESS_COMMENT_COLUMN_NAME = "STATE_COMMENT";
  static final String PROCESS_EXEC_CNT_COLUMN_NAME = "EXEC_CNT";
}
