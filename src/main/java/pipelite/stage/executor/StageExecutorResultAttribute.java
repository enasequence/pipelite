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
package pipelite.stage.executor;

public class StageExecutorResultAttribute {

  public static final String EXIT_CODE = "exit code";
  public static final String COMMAND = "command";
  public static final String HOST = "host";
  public static final String EXEC_HOST = "execution host";
  public static final String CPU_TIME = "cpu time";
  public static final String AVG_MEM = "avg mem";
  public static final String MAX_MEM = "max mem";
  public static final String MESSAGE = "message";
  public static final String EXCEPTION = "exception";

  private StageExecutorResultAttribute() {}
}
