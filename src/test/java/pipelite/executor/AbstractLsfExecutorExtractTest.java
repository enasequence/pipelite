/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;

public class AbstractLsfExecutorExtractTest {

  @Test
  public void extractBsubJobIdSubmitted() {
    assertThat(
            AbstractLsfExecutor.extractBsubJobIdSubmitted(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");
    assertThat(AbstractLsfExecutor.extractBsubJobIdSubmitted("Job <2848143> is submitted "))
        .isEqualTo("2848143");
    assertThrows(
        PipeliteException.class, () -> AbstractLsfExecutor.extractBsubJobIdSubmitted("INVALID"));
  }

  @Test
  public void extractBjobsJobIdNotFound() {
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("Job <345654> is not found."))
        .isEqualTo("345654");
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("Job <345654> is not found"))
        .isEqualTo("345654");
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("Job <345654> is ")).isNull();
    assertThat(AbstractLsfExecutor.extractBjobsJobIdNotFound("INVALID")).isNull();
  }

  @Test
  public void extractBhistExitCode() {
    assertThat(AbstractLsfExecutor.extractBhistExitCode("Exited with exit code 1")).isEqualTo("1");
    assertThat(AbstractLsfExecutor.extractBhistExitCode("Exited with exit code 3.")).isEqualTo("3");
    assertThat(AbstractLsfExecutor.extractBhistExitCode("INVALID")).isNull();
  }

  @Test
  public void extractBjobsResult() {
    AbstractLsfExecutor.BjobsResult result =
        AbstractLsfExecutor.extractBjobsResult("861487|PEND|-|-|-|-|-\n");
    assertThat(result.getJobId()).isEqualTo("861487");
    assertThat(result.getResult().isActive()).isTrue();
  }

  @Test
  public void extractBjobsResults() {
    List<AbstractLsfExecutor.BjobsResult> result =
        AbstractLsfExecutor.extractBjobsResults(
            "861487|PEND|-|-|-|-|-\n" + "861488|PEND|-|-|-|-|-\n" + "861489|PEND|-|-|-|-|-");
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getJobId()).isEqualTo("861487");
    assertThat(result.get(1).getJobId()).isEqualTo("861488");
    assertThat(result.get(2).getJobId()).isEqualTo("861489");
    assertThat(result.get(0).getResult().isActive()).isTrue();
    assertThat(result.get(1).getResult().isActive()).isTrue();
    assertThat(result.get(2).getResult().isActive()).isTrue();

    result =
        AbstractLsfExecutor.extractBjobsResults(
            "872793|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872794|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872795|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n");

    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getJobId()).isEqualTo("872793");
    assertThat(result.get(1).getJobId()).isEqualTo("872794");
    assertThat(result.get(2).getJobId()).isEqualTo("872795");
    assertThat(result.get(0).getResult().isSuccess()).isTrue();
    assertThat(result.get(1).getResult().isSuccess()).isTrue();
    assertThat(result.get(2).getResult().isSuccess()).isTrue();

    result =
        AbstractLsfExecutor.extractBjobsResults(
            "873206|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "873207|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "873209|EXIT|127|0.0 second(s)|-|-|hx-noah-10-04");

    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getJobId()).isEqualTo("873206");
    assertThat(result.get(1).getJobId()).isEqualTo("873207");
    assertThat(result.get(2).getJobId()).isEqualTo("873209");
    assertThat(result.get(0).getResult().isError()).isTrue();
    assertThat(result.get(1).getResult().isError()).isTrue();
    assertThat(result.get(2).getResult().isError()).isTrue();
  }

  @Test
  public void extractBhistResult() {
    StageExecutorResult result =
        AbstractLsfExecutor.extractBhistResult(
            "Summary of time in seconds spent in various states:\n"
                + "JOBID   USER    JOB_NAME  PEND    PSUSP   RUN     USUSP   SSUSP   UNKWN   TOTAL\n"
                + "873209  rasko   fdfs      1       0       0       0       0       0       1\n"
                + "\n"
                + "[rasko@noah-login-01 ~]$ bhist -l 873209\n"
                + "\n"
                + "Job <873209>, User <rasko>, Project <default>, Command <fdfs>, Esub <esub>\n"
                + "Sun Jan 10 17:50:11: Submitted from host <noah-login-01>, to Queue <research-rh\n"
                + "                     74>, CWD <$HOME>, Requested Resources <rusage[numcpus=1:du\n"
                + "                     ration=480]>;\n"
                + "Sun Jan 10 17:50:12: Dispatched to <hx-noah-10-04>, Effective RES_REQ <select[t\n"
                + "                     ype == local] order[r15s:pg] rusage[numcpus=1.00:duration=\n"
                + "                     8h:decay=0] span[hosts=1] >;\n"
                + "Sun Jan 10 17:50:12: Starting (Pid 87178);\n"
                + "Sun Jan 10 17:50:12: Running with execution home </homes/rasko>, Execution CWD\n"
                + "                     </homes/rasko>, Execution Pid <87178>;\n"
                + "Sun Jan 10 17:50:12: Exited with exit code 127. The CPU time used is 0.0 second\n"
                + "                     s;\n"
                + "Sun Jan 10 17:50:12: Completed <exit>;\n"
                + "\n"
                + "\n"
                + " CORELIMIT\n"
                + "      0 M\n"
                + "\n"
                + "Summary of time in seconds spent in various states by  Sun Jan 10 17:50:12\n"
                + "  PEND     PSUSP    RUN      USUSP    SSUSP    UNKWN    TOTAL\n"
                + "  1        0        0        0        0        0        1");
    assertThat(result.isError()).isTrue();

    result =
        AbstractLsfExecutor.extractBhistResult(
            "Job <872795>, User <rasko>, Project <default>, Command <echo hello>, Esub <esub\n"
                + "                     >\n"
                + "Sun Jan 10 17:47:38: Submitted from host <noah-login-01>, to Queue <research-rh\n"
                + "                     74>, CWD <$HOME>, Requested Resources <rusage[numcpus=1:du\n"
                + "                     ration=480]>;\n"
                + "Sun Jan 10 17:47:39: Dispatched to <hx-noah-05-14>, Effective RES_REQ <select[t\n"
                + "                     ype == local] order[r15s:pg] rusage[numcpus=1.00:duration=\n"
                + "                     8h:decay=0] span[hosts=1] >;\n"
                + "Sun Jan 10 17:47:39: Starting (Pid 110245);\n"
                + "Sun Jan 10 17:47:39: Running with execution home </homes/rasko>, Execution CWD\n"
                + "                     </homes/rasko>, Execution Pid <110245>;\n"
                + "Sun Jan 10 17:47:39: Done successfully. The CPU time used is 0.0 seconds;\n"
                + "Sun Jan 10 17:47:40: Post job process done successfully;\n"
                + "\n"
                + "\n"
                + " CORELIMIT\n"
                + "      0 M\n"
                + "\n"
                + "Summary of time in seconds spent in various states by  Sun Jan 10 17:47:40\n"
                + "  PEND     PSUSP    RUN      USUSP    SSUSP    UNKWN    TOTAL\n"
                + "  1        0        0        0        0        0        1");
    assertThat(result.isSuccess()).isTrue();
  }
}
