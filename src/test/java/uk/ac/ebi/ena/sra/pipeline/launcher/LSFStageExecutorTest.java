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
import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

public class LSFStageExecutorTest {
  private ResultTranslator makeResultTranslator() {
    return new ResultTranslator(
        new ExecutionResult[] {
          new ExecutionResult() {
            @Override
            public RESULT_TYPE getType() {
              return null;
            }

            @Override
            public byte getExitCode() {
              return 0;
            }

            @Override
            public Class<? extends Throwable> getCause() {
              return null;
            }

            @Override
            public String getMessage() {
              return null;
            }
          }
        });
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

  private StageInstance makeDefaultStageInstance() {
    return new StageInstance() {
      {
        setEnabled(true);
        setPropertiesPass(new String[] {});
        setCPUCores(1);
        setMemoryLimit(-1);
        setPropertiesPass(new String[] {});
      }
    };
  }

  @Test
  public void testNoQueue() throws IOException {
    ResultTranslator translator = makeResultTranslator();

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
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.execute(makeDefaultStageInstance());

    Assert.assertFalse(se.get_info().getCommandline().contains("-q "));
  }

  @Test
  public void testQueue() throws IOException {
    ResultTranslator translator = makeResultTranslator();

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
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.execute(makeDefaultStageInstance());

    Assert.assertTrue(se.get_info().getCommandline().contains("-q queue"));
  }

  @Test
  public void testStageSpecificQueue() throws IOException {
    ResultTranslator translator = makeResultTranslator();

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
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(cfg_stg);

    se.execute(makeDefaultStageInstance());

    Assert.assertTrue(se.get_info().getCommandline().contains("-q LSFQUEUE"));
  }

  @Test
  public void stageSpecificConfig() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

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
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemoryLimit(2000);
            setCPUCores(12);
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -M 2000 -R rusage[mem=2000:duration=14]"));
    Assert.assertTrue(cmdl.contains(" -n 12"));
    Assert.assertTrue(cmdl.contains(" -q LSFQUEUE"));
    Assert.assertTrue(cmdl.contains(" -oo " + tmpd_stg));
    Assert.assertTrue(cmdl.contains(" -eo " + tmpd_stg));
  }

  @Test
  public void defaultMemoryCoresReserveConfig() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, -1, -1, "NOFILE", "NOPATH", new String[] {}, cfg_def);

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
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemoryLimit(-1);
            setCPUCores(-1);
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -M 1700 -R rusage[mem=1700:duration=60]"));
    Assert.assertTrue(cmdl.contains(" -n 1"));
    Assert.assertTrue(cmdl.contains(" -q LSFQUEUE"));
    Assert.assertTrue(cmdl.contains(" -oo " + tmpd_stg));
    Assert.assertTrue(cmdl.contains(" -eo " + tmpd_stg));
  }

  @Test
  public void genericConfig() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(makeDefaultStageInstance());

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -M 2340 -R rusage[mem=2340:duration=9]"));
    Assert.assertTrue(cmdl.contains(" -n 1 "));
  }

  @Test
  public void genericConfigNoConfCall() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.execute(makeDefaultStageInstance());

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -M 2340 -R rusage[mem=2340:duration=9]"));
    Assert.assertTrue(cmdl.contains(" -n 1 "));
  }

  @Test
  public void javaMemory() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemoryLimit(1700);
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -Xmx200M"));
  }

  @Test
  public void javaMemoryNotSet() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -M 2340 -R rusage[mem=2340:duration=9]"));
    Assert.assertTrue(cmdl.contains(" -Xmx840M"));
  }

  @Test
  public void memoryBelow1500() throws IOException {
    ResultTranslator translator = makeResultTranslator();
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
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {}, cfg_def);

    se.configure(null);

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemoryLimit(1400);
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(!cmdl.contains(" -Xmx"));
  }

  @Test
  public void propertiesPassStageSpecific() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {"user.dir"}, cfg_def);

    se.configure(null);

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {"user.country"});
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -Duser.country="));
    Assert.assertTrue(cmdl.contains(" -Duser.dir="));
  }

  @Test
  public void propertiesPassGeneric() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, "NOFILE", "NOPATH", new String[] {"user.dir"}, cfg_def);

    se.configure(null);

    se.execute(makeDefaultStageInstance());

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -Duser.dir="));
  }

  @Test
  public void prefixAndSource() throws IOException {
    ResultTranslator translator = makeResultTranslator();
    LSFExecutorConfig cfg_def = makeDefaultConfig();

    String prefix = "NOFILE";
    String source = "NOPATH";
    LSFStageExecutor se =
        new LSFStageExecutor(
            "TEST", translator, 2340, 3, prefix, source, new String[] {"user.dir"}, cfg_def);

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -D" + prefix + "=" + source));
  }
}
