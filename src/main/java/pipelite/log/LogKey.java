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
package pipelite.log;

import com.google.common.flogger.MetadataKey;
import pipelite.stage.StageExecutionResultType;

public class LogKey {

  private LogKey() {}

  public static final MetadataKey<String> SERVICE_NAME =
      MetadataKey.single("service_name", String.class);

  public static final MetadataKey<String> LAUNCHER_NAME =
      MetadataKey.single("launcher_name", String.class);

  public static final MetadataKey<String> PIPELINE_NAME =
      MetadataKey.single("pipeline_name", String.class);

  public static final MetadataKey<String> PROCESS_ID =
      MetadataKey.single("process_id", String.class);

  public static final MetadataKey<String> STAGE_NAME =
      MetadataKey.single("stage_name", String.class);

  public static final MetadataKey<StageExecutionResultType> STAGE_EXECUTION_RESULT_TYPE =
      MetadataKey.single("stage_execution_result_type", StageExecutionResultType.class);

  public static final MetadataKey<Integer> STAGE_EXECUTION_COUNT =
      MetadataKey.single("stage_execution_count", Integer.class);
}
