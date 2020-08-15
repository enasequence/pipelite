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

import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public interface StorageBackend {
  class StorageException extends Exception {
    public StorageException() {
      super();
    }

    public StorageException(String arg0, Throwable arg1) {
      super(arg0, arg1);
    }

    public StorageException(String arg0) {
      super(arg0);
    }

    public StorageException(Throwable arg0) {
      super(arg0);
    }

    private static final long serialVersionUID = 1L;
  }

  void load(StageInstance si) throws StorageException;

  void save(StageInstance si) throws StorageException;

  void load(PipeliteState ps) throws StorageException;

  void save(PipeliteState ps) throws StorageException;

  String getExecutionId() throws StorageException;

  void load(ExecutionInstance instance) throws StorageException;

  void save(ExecutionInstance instance) throws StorageException;

  //   void load( OracleProcessLogBean bean ) throws StorageException;
  void save(ProcessLogBean bean) throws StorageException;

  void flush() throws StorageException;

  void close() throws StorageException;
}
