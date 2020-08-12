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

import org.junit.Assert;
import org.junit.Test;
import pipelite.task.result.TaskExecutionResultTranslator;
import pipelite.task.result.TaskExecutionResult;
import pipelite.task.result.TaskExecutionResultType;

public class DetachedStageExecutorTest {
  @Test
  public void javaMemory() {
    DetachedStageExecutor se =
        new DetachedStageExecutor(
            "TEST",
            new TaskExecutionResultTranslator(
                new TaskExecutionResult[] {
                  new TaskExecutionResult() {
                    @Override
                    public TaskExecutionResultType getExecutionResultType() {
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
                    public String getExecutionResult() {
                      return null;
                    }
                  }
                }),
            "NOFILE",
            "NOPATH",
            new String[] {});

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setMemoryLimit(2000);
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -Xmx2000M"));
  }

  @Test
  public void javaMemoryNotSet() {
    DetachedStageExecutor se =
        new DetachedStageExecutor(
            "TEST",
            new TaskExecutionResultTranslator(
                new TaskExecutionResult[] {
                  new TaskExecutionResult() {
                    @Override
                    public TaskExecutionResultType getExecutionResultType() {
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
                    public String getExecutionResult() {
                      return null;
                    }
                  }
                }),
            "NOFILE",
            "NOPATH",
            new String[] {});

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(!cmdl.contains(" -Xmx2000M"));
  }

  @Test
  public void prefixAndSource() {
    String prefix = "NOFILE";
    String source = "NOPATH";
    DetachedStageExecutor se =
        new DetachedStageExecutor(
            "TEST",
            new TaskExecutionResultTranslator(
                new TaskExecutionResult[] {
                  new TaskExecutionResult() {
                    @Override
                    public TaskExecutionResultType getExecutionResultType() {
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
                    public String getExecutionResult() {
                      return null;
                    }
                  }
                }),
            prefix,
            source,
            new String[] {});

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

  @Test
  public void propertiesPassStageSpecific() {
    String prefix = "NOFILE";
    String source = "NOPATH";
    DetachedStageExecutor se =
        new DetachedStageExecutor(
            "TEST",
            new TaskExecutionResultTranslator(
                new TaskExecutionResult[] {
                  new TaskExecutionResult() {
                    @Override
                    public TaskExecutionResultType getExecutionResultType() {
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
                    public String getExecutionResult() {
                      return null;
                    }
                  }
                }),
            prefix,
            source,
            new String[] {"user.dir"});

    se.configure(null);

    se.execute(
        new StageInstance() {
          {
            setEnabled(true);
            setPropertiesPass(new String[] {});
            setPropertiesPass(new String[] {"user.country"});
          }
        });

    String cmdl = se.get_info().getCommandline();
    Assert.assertTrue(cmdl.contains(" -Duser.country="));
    Assert.assertTrue(cmdl.contains(" -Duser.dir="));
  }
}