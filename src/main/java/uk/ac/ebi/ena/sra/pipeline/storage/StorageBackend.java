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

  String getExecutionId() throws StorageException;

  void flush() throws StorageException;

  void close() throws StorageException;
}
