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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;
import uk.ac.ebi.ena.sra.pipeline.storage.EnumStorage.ProcessIdFactory;

public class EnumStorageTest {
  static final Logger log = Logger.getLogger(EnumStorageTest.class);

  @BeforeClass
  public static void beforeClass() {
    PropertyConfigurator.configure("resource/test.log4j.properties");
  }

  public enum __testovye_shagee implements Stage {
    ONE,
    TWO,
    THREE;

    @Override
    public Class<? extends StageTask> getTaskClass() {
      return (new StageTask() {
            @Override
            public void unwind() {}

            @Override
            public void init(Object id) {}

            @Override
            public void execute() {}
          })
          .getClass();
    }

    @Override
    public Stage getDependsOn() {
      return null;
    }

    @Override
    public String getDescription() {
      return toString();
    }
  }

  @Test
  public void test() throws NoSuchFieldException {
    EnumStorage<__testovye_shagee> es = new EnumStorage<>(__testovye_shagee.class);
    es.setProcessIdFactory(
        new ProcessIdFactory() {
          public String getProcessId() {
            return "ИДЕНТИФИКАТОР";
          }
        });
    es.setPipelineName("ТЕСТОВАЯ_ЛИНИЯ");
    List<StageInstance> si_list = new ArrayList<>();
    Stream.of(__testovye_shagee.values())
        .forEach(
            stage -> {
                StageInstance si = new StageInstance();
                si.setStageName(stage.toString());
                es.load(si);
                log.info(si);
                si_list.add(si);
            });
    log.info(Arrays.asList(si_list));
    Assert.assertEquals(__testovye_shagee.values().length, si_list.size());

    ProcessLogBean lb = Mockito.spy(new ProcessLogBean());

    lb.setPipelineName(es.getPipelineName());
    es.save(lb);

    Mockito.verify(lb, Mockito.atLeastOnce()).getPipelineName();
  }
}
