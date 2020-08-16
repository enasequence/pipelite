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

import java.io.IOException;
import java.nio.file.Files;

import org.junit.jupiter.api.Test;
import pipelite.task.instance.TaskInstance;
import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import pipelite.task.result.resolver.TaskExecutionResultResolver;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;

import static org.junit.jupiter.api.Assertions.*;

public class LSFStageExecutorTest {
  private TaskExecutionResultExceptionResolver resolver() {
      return TaskExecutionResultResolver.DEFAULT_EXCEPTION_RESOLVER;
  }

  private LSFExecutorConfig makeDefaultConfig() throws IOException {
    String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();

    return new LSFExecutorConfig() {
      @Override
      public int getLSFMemoryReservationTimeout() {
        return 9;
      }

      @Override
      public String getLsfQueue() {
        return "LSFQUEUE";
      }

      @Override
      public String getLsfOutputPath() {
        return tmpd_def;
      }
    };
  }

  private TaskInstance makeDefaultStageInstance() {
    return new TaskInstance() {
      {
        setEnabled(true);
        setPropertiesPass(new String[] {});
        setCores(1);
        setMemory(-1);
        setPropertiesPass(new String[] {});
      }
    };
  }

  @Test
  public void testNoQueue() throws IOException {
    String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();
    LSFExecutorConfig cfg_def =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return 9;
          }

          @Override
          public String getLsfQueue() {
            return null;
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_def;
          }
        };

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.execute(makeDefaultStageInstance());

    assertFalse(se.get_info().getCommandline().contains("-q "));
  }

  @Test
  public void testQueue() throws IOException {
    String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();
    LSFExecutorConfig cfg_def =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return 9;
          }

          @Override
          public String getLsfQueue() {
            return "queue";
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_def;
          }
        };

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.execute(makeDefaultStageInstance());

    assertTrue(se.get_info().getCommandline().contains("-q queue"));
  }

  @Test
  public void testStageSpecificQueue() throws IOException {

    String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();
    LSFExecutorConfig cfg_def =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return 9;
          }

          @Override
          public String getLsfQueue() {
            return "queue";
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_def;
          }
        };

    String tmpd_stg = Files.createTempDirectory("LSF-TEST-OUTPUT-STG").toString();
    LSFExecutorConfig cfg_stg =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return 14;
          }

          @Override
          public String getLsfQueue() {
            return "LSFQUEUE";
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_stg;
          }
        };

    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(cfg_stg);

    se.execute(makeDefaultStageInstance());

    assertTrue(se.get_info().getCommandline().contains("-q LSFQUEUE"));
  }

  @Test
  public void stageSpecificConfig() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    String tmpd_stg = Files.createTempDirectory("LSF-TEST-OUTPUT-STG").toString();
    LSFExecutorConfig cfg_stg =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return 14;
          }

          @Override
          public String getLsfQueue() {
            return "LSFQUEUE";
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_stg;
          }
        };

    se.configure(cfg_stg);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemory(2000);
            setCores(12);
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -M 2000 -R rusage[mem=2000:duration=14]"));
    assertTrue(cmdl.contains(" -n 12"));
    assertTrue(cmdl.contains(" -q LSFQUEUE"));
    assertTrue(cmdl.contains(" -oo " + tmpd_stg));
    assertTrue(cmdl.contains(" -eo " + tmpd_stg));
  }

  @Test
  public void defaultMemoryCoresReserveConfig() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), -1, -1, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    String tmpd_stg = Files.createTempDirectory("LSF-TEST-OUTPUT-STG").toString();
    LSFExecutorConfig cfg_stg =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return -1;
          }

          @Override
          public String getLsfQueue() {
            return "LSFQUEUE";
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_stg;
          }
        };

    se.configure(cfg_stg);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemory(-1);
            setCores(-1);
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -M 1700 -R rusage[mem=1700:duration=60]"));
    assertTrue(cmdl.contains(" -n 1"));
    assertTrue(cmdl.contains(" -q LSFQUEUE"));
    assertTrue(cmdl.contains(" -oo " + tmpd_stg));
    assertTrue(cmdl.contains(" -eo " + tmpd_stg));
  }

  @Test
  public void genericConfig() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(makeDefaultStageInstance());

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -M 2340 -R rusage[mem=2340:duration=9]"));
    assertTrue(cmdl.contains(" -n 1 "));
  }

  @Test
  public void genericConfigNoConfCall() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.execute(makeDefaultStageInstance());

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -M 2340 -R rusage[mem=2340:duration=9]"));
    assertTrue(cmdl.contains(" -n 1 "));
  }

  @Test
  public void javaMemory() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemory(1700);
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -Xmx200M"));
  }

  @Test
  public void javaMemoryNotSet() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -M 2340 -R rusage[mem=2340:duration=9]"));
    assertTrue(cmdl.contains(" -Xmx840M"));
  }

  @Test
  public void memoryBelow1500() throws IOException {

    String tmpd_def = Files.createTempDirectory("LSF-TEST-OUTPUT-DEF").toString();
    LSFExecutorConfig cfg_def =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return 9;
          }

          @Override
          public String getLsfQueue() {
            return "LSFQUEUE";
          }

          @Override
          public String getLsfOutputPath() {
            return tmpd_def;
          }
        };
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemory(1400);
          }
        });

    String cmdl = se.get_info().getCommandline();
      assertFalse(cmdl.contains(" -Xmx"));
  }

  @Test
  public void propertiesPassStageSpecific() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {"user.dir"}, cfg_def);

    se.configure(null);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {"user.country"});
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -Duser.country="));
    assertTrue(cmdl.contains(" -Duser.dir="));
  }

  @Test
  public void propertiesPassGeneric() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, "NOFILE", "NOPATH", new String[] {"user.dir"}, cfg_def);

    se.configure(null);

    se.execute(makeDefaultStageInstance());

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -Duser.dir="));
  }

  @Test
  public void prefixAndSource() throws IOException {

    LSFExecutorConfig cfg_def = makeDefaultConfig();

    String prefix = "NOFILE";
    String source = "NOPATH";
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", resolver(), 2340, 3, prefix, source, new String[] {"user.dir"}, cfg_def);

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -D" + prefix + "=" + source));
  }
}
