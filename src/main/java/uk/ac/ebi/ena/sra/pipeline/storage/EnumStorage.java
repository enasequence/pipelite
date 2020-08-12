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

import java.util.stream.Stream;
import org.apache.log4j.Logger;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState.State;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;

public class EnumStorage<T extends Enum<T> & Stage> implements StorageBackend {
  public interface ProcessIdFactory {
    public default String getProcessId() {
      return null;
    };
  };

  Logger log = Logger.getLogger(getClass());
  Class<T> e;
  private String pipeline_name;
  private ProcessIdFactory idf = new ProcessIdFactory() {};

  public String getPipelineName() {
    return this.pipeline_name;
  }

  public void setPipelineName(String pipeline_name) {
    this.pipeline_name = pipeline_name;
  }

  public EnumStorage(Class<T> e) {
    this.e = e;
  }

  @Override
  public void load(PipeliteState ps) throws StorageException {
    ps.setExecCount(0);
    ps.setState(State.ACTIVE);
    ps.setPriority(0);
    log.info(ps);
  }

  @Override
  public void save(PipeliteState ps) throws StorageException {
    log.info(ps);
  }

  @Override
  public void save(StageInstance si) throws StorageException {
    log.info(si);
  }

  @Override
  public void load(ExecutionInstance ei) throws StorageException {
    log.info(ei);
  }

  @Override
  public void save(ExecutionInstance ei) throws StorageException {
    log.info(ei);
  }

  @Override
  public void save(ProcessLogBean bean) throws StorageException {
    try {
      bean.getPipelineName();
    } catch (NoSuchFieldException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    log.error(bean);
  }

  @Override
  public void flush() throws StorageException {
    // do nothing
  }

  @Override
  public void close() throws StorageException {
    // do nothing
  }

  @Override
  public void load(StageInstance si) throws StorageException {
    Stream.of(e.getEnumConstants())
        .filter(s -> s.toString().equals(si.getStageName()))
        .findFirst()
        .ifPresent(
            s -> {
              si.setPipelineName(getPipelineName());
              si.setProcessID(getProcessIdFactory().getProcessId());
              log.info(si);
            });
  }

  public ProcessIdFactory getProcessIdFactory() {
    return idf;
  }

  public void setProcessIdFactory(ProcessIdFactory idf) {
    this.idf = idf;
  }

  @Override
  public String getExecutionId() throws StorageException {
    return "1";
  }
}
