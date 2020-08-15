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
import pipelite.task.instance.LatestTaskExecution;
import pipelite.process.instance.ProcessInstance;
import pipelite.process.state.ProcessExecutionState;
import pipelite.task.instance.TaskInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;

public class EnumStorage<T extends Enum<T> & Stage> implements StorageBackend {
  public interface ProcessIdFactory {
    default String getProcessId() {
      return null;
    }
  }

    final Logger log = Logger.getLogger(getClass());
  final Class<T> e;
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
  public void load(ProcessInstance ps) {
    ps.setExecutionCount(0);
    ps.setState(ProcessExecutionState.ACTIVE);
    ps.setPriority(0);
    log.info(ps);
  }

  @Override
  public void save(ProcessInstance ps) {
    log.info(ps);
  }

  @Override
  public void save(TaskInstance si) {
    log.info(si);
  }

  @Override
  public void load(LatestTaskExecution ei) {
    log.info(ei);
  }

  @Override
  public void save(LatestTaskExecution ei) {
    log.info(ei);
  }

  @Override
  public void save(ProcessLogBean bean) {
    try {
      bean.getPipelineName();
    } catch (NoSuchFieldException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    log.error(bean);
  }

  @Override
  public void flush() {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void load(TaskInstance si) {
    Stream.of(e.getEnumConstants())
        .filter(s -> s.toString().equals(si.getTaskName()))
        .findFirst()
        .ifPresent(
            s -> {
              si.setProcessName(getPipelineName());
              si.setProcessId(getProcessIdFactory().getProcessId());
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
  public String getExecutionId() {
    return "1";
  }
}
