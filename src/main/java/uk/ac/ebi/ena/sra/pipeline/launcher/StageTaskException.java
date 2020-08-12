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
package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.storage.ProcessLogBean;

public class StageTaskException extends Exception {
  private static final long serialVersionUID = 1L;

  ProcessLogBean log_bean;

  public StageTaskException(String message, Throwable cause, ProcessLogBean log_bean) {
    super(message, cause);
    this.log_bean = log_bean;
  }

  public void setLogBean(ProcessLogBean log_bean) {
    this.log_bean = log_bean;
  }

  public ProcessLogBean getLogBean() {
    return log_bean;
  }
}