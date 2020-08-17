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

import pipelite.task.instance.LatestTaskExecution;
import pipelite.process.instance.ProcessInstance;
import pipelite.task.instance.TaskInstance;

public interface StorageBackend {
  class StorageException extends Exception {

    public StorageException(String arg0) {
      super(arg0);
    }

    public StorageException(Throwable arg0) {
      super(arg0);
    }

    private static final long serialVersionUID = 1L;
  }

  void load(TaskInstance si) throws StorageException;

  void save(TaskInstance si) throws StorageException;


  void load(ProcessInstance ps) throws StorageException;

  void save(ProcessInstance ps) throws StorageException;


  String getExecutionId() throws StorageException;

  void load(LatestTaskExecution instance) throws StorageException;

  void save(LatestTaskExecution instance) throws StorageException;

  //   void load( OracleProcessLogBean bean ) throws StorageException;
  void save(ProcessLogBean bean) throws StorageException;

  void flush() throws StorageException;

  void close() throws StorageException;
}
