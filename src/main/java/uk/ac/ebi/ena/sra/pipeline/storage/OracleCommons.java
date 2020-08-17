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
  String PIPELINE_PROCESS_TABLE_NAME = "PIPELITE_PROCESS";
  String PIPELINE_STAGE_TABLE_NAME = "PIPELITE_STAGE";

  String PIPELINE_COLUMN_NAME = "PIPELINE_NAME";
  String PROCESS_COLUMN_NAME = "PROCESS_ID";
  String STAGE_NAME_COLUMN_NAME = "STAGE_NAME";

  String ATTEMPT_COLUMN_NAME = "EXEC_CNT";
  String ENABLED_COLUMN_NAME = "ENABLED";

  String EXEC_ID_SEQUENCE = "PIPELITE_EXEC_ID_SEQ";
  String EXEC_ID_COLUMN_NAME = "EXEC_ID";
  String EXEC_START_COLUMN_NAME = "EXEC_START";
  String EXEC_DATE_COLUMN_NAME = "EXEC_DATE";
  String EXEC_RESULT_COLUMN_NAME = "EXEC_RESULT";
  String EXEC_RESULT_TYPE_COLUMN_NAME = "EXEC_RESULT_TYPE";
  String EXEC_STDOUT_COLUMN_NAME = "EXEC_STDOUT";
  String EXEC_STDERR_COLUMN_NAME = "EXEC_STDERR";
  String EXEC_CMDLINE_COLUMN_NAME = "EXEC_CMD_LINE";

  String PROCESS_PRIORITY_COLUMN_NAME = "PRIORITY";
  String PROCESS_STATE_COLUMN_NAME = "STATE";
  String PROCESS_COMMENT_COLUMN_NAME = "STATE_COMMENT";
}
