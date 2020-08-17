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

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.resolver.ExceptionResolver;
import pipelite.task.instance.TaskInstance;
import pipelite.resolver.TaskExecutionResultResolver;

public class DetachedStageExecutorTest {

  private static ExceptionResolver resolver = new DefaultExceptionResolver();

  @Test
  public void javaMemory() {
    DetachedStageExecutor se =
        new DetachedStageExecutor("TEST", resolver, "NOFILE", "NOPATH", new String[] {});

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setJavaSystemProperties(new String[] {});
            setMemory(2000);
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -Xmx2000M"));
  }

  @Test
  public void javaMemoryNotSet() {
    DetachedStageExecutor se =
        new DetachedStageExecutor("TEST", resolver, "NOFILE", "NOPATH", new String[] {});

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setJavaSystemProperties(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertFalse(cmdl.contains(" -Xmx2000M"));
  }

  @Test
  public void prefixAndSource() {
    String prefix = "NOFILE";
    String source = "NOPATH";
    DetachedStageExecutor se =
        new DetachedStageExecutor("TEST", resolver, prefix, source, new String[] {});

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setJavaSystemProperties(new String[] {});
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -D" + prefix + "=" + source));
  }

  @Test
  public void testJavaSystemProperties() {
    String prefix = "NOFILE";
    String source = "NOPATH";
    DetachedStageExecutor se =
        new DetachedStageExecutor("TEST", resolver, prefix, source, new String[] {"user.dir"});

    se.execute(
        new TaskInstance() {
          {
            setEnabled(true);
            setJavaSystemProperties(new String[] {});
            setJavaSystemProperties(new String[] {"user.country"});
          }
        });

    String cmdl = se.get_info().getCommandline();
    assertTrue(cmdl.contains(" -Duser.country="));
    assertTrue(cmdl.contains(" -Duser.dir="));
  }
}
