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
  public void extractDefaultLsfJobExitCode() {
    assertThat(AbstractLsfExecutor.extracLsfJobExitCode("Exited with exit code 1")).isEqualTo(1);
    assertThat(AbstractLsfExecutor.extracLsfJobExitCode("Exited with exit code 3.")).isEqualTo(3);
    assertThat(AbstractLsfExecutor.extracLsfJobExitCode("INVALID")).isNull();
  }

  @Test
  public void extractBjobsResult() {
    AbstractLsfExecutor.JobResult result =
        AbstractLsfExecutor.extractBjobsResult("861487|PEND|-|-|-|-|-\n");
    assertThat(result.jobId).isEqualTo("861487");
    assertThat(result.result.isActive()).isTrue();
  }

  @Test
  public void extractBjobsResults() {
    List<AbstractLsfExecutor.JobResult> result =
        AbstractLsfExecutor.extractBjobsResults(
            "861487|PEND|-|-|-|-|-\n" + "861488|PEND|-|-|-|-|-\n" + "861489|PEND|-|-|-|-|-");
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).jobId).isEqualTo("861487");
    assertThat(result.get(1).jobId).isEqualTo("861488");
    assertThat(result.get(2).jobId).isEqualTo("861489");
    assertThat(result.get(0).result.isActive()).isTrue();
    assertThat(result.get(1).result.isActive()).isTrue();
    assertThat(result.get(2).result.isActive()).isTrue();

    result =
        AbstractLsfExecutor.extractBjobsResults(
            "872793|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872794|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872795|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n");

    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).jobId).isEqualTo("872793");
    assertThat(result.get(1).jobId).isEqualTo("872794");
    assertThat(result.get(2).jobId).isEqualTo("872795");
    assertThat(result.get(0).result.isSuccess()).isTrue();
    assertThat(result.get(1).result.isSuccess()).isTrue();
    assertThat(result.get(2).result.isSuccess()).isTrue();

    result =
        AbstractLsfExecutor.extractBjobsResults(
            "873206|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "873207|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "Job <6065212> is not found\n"
                + "873209|EXIT|127|0.0 second(s)|-|-|hx-noah-10-04\n"
                + "Job <6065212> is not found\n");

    assertThat(result.size()).isEqualTo(5);
    assertThat(result.get(0).jobId).isEqualTo("873206");
    assertThat(result.get(1).jobId).isEqualTo("873207");
    assertThat(result.get(2).jobId).isEqualTo("6065212");
    assertThat(result.get(3).jobId).isEqualTo("873209");
    assertThat(result.get(4).jobId).isEqualTo("6065212");
    assertThat(result.get(0).result.isError()).isTrue();
    assertThat(result.get(1).result.isError()).isTrue();
    assertThat(result.get(2).result).isNull();
    assertThat(result.get(3).result.isError()).isTrue();
    assertThat(result.get(4).result).isNull();
  }

  @Test
  public void extractLsfJobResultBhistError() {
    StageExecutorResult result =
        AbstractLsfExecutor.extractLsfJobResult(
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
  }

  @Test
  public void extractLsfJobResultBhistSuccess() {
    StageExecutorResult result =
        AbstractLsfExecutor.extractLsfJobResult(
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

  @Test
  public void extractLsfJobResultOutFileError() {
    StageExecutorResult result =
        AbstractLsfExecutor.extractLsfJobResult(
            "\n"
                + "\n"
                + "Job <dfsddf> was submitted from host <noah-login-02> by user <rasko> in cluster <EBI> at Sun Jan 24 18:06:13 2021\n"
                + "Job was executed on host(s) <hx-noah-24-04>, in queue <research-rh74>, as user <rasko> in cluster <EBI> at Sun Jan 24 18:06:14 2021\n"
                + "</homes/rasko> was used as the home directory.\n"
                + "</homes/rasko> was used as the working directory.\n"
                + "Started at Sun Jan 24 18:06:14 2021\n"
                + "Terminated at Sun Jan 24 18:06:14 2021\n"
                + "Results reported at Sun Jan 24 18:06:14 2021\n"
                + "\n"
                + "Your job looked like:\n"
                + "\n"
                + "------------------------------------------------------------\n"
                + "# LSBATCH: User input\n"
                + "dfsddf\n"
                + "------------------------------------------------------------\n"
                + "\n"
                + "Exited with exit code 127.\n"
                + "\n"
                + "Resource usage summary:\n"
                + "\n"
                + "    CPU time :                                   0.03 sec.\n"
                + "    Max Memory :                                 -\n"
                + "    Average Memory :                             -\n"
                + "    Total Requested Memory :                     -\n"
                + "    Delta Memory :                               -\n"
                + "    Max Swap :                                   -\n"
                + "    Max Processes :                              -\n"
                + "    Max Threads :                                -\n"
                + "    Run time :                                   0 sec.\n"
                + "    Turnaround time :                            1 sec.\n"
                + "\n"
                + "The output (if any) follows:\n"
                + "\n"
                + "/ebi/lsf/ebi-spool2/01/1611511573.6138156: line 8: dfsddf: command not found\n"
                + "\n");
    assertThat(result.isError()).isTrue();
  }

  @Test
  public void extractLsfJobResultOutFileSuccess() {
    StageExecutorResult result =
        AbstractLsfExecutor.extractLsfJobResult(
            "\n"
                + "\n"
                + "Job <echo test> was submitted from host <noah-login-02> by user <rasko> in cluster <EBI> at Sun Jan 24 18:03:50 2021\n"
                + "Job was executed on host(s) <hx-noah-51-01>, in queue <research-rh74>, as user <rasko> in cluster <EBI> at Sun Jan 24 18:03:51 2021\n"
                + "</homes/rasko> was used as the home directory.\n"
                + "</homes/rasko> was used as the working directory.\n"
                + "Started at Sun Jan 24 18:03:51 2021\n"
                + "Terminated at Sun Jan 24 18:03:51 2021\n"
                + "Results reported at Sun Jan 24 18:03:51 2021\n"
                + "\n"
                + "Your job looked like:\n"
                + "\n"
                + "------------------------------------------------------------\n"
                + "# LSBATCH: User input\n"
                + "echo test\n"
                + "------------------------------------------------------------\n"
                + "\n"
                + "Successfully completed.\n"
                + "\n"
                + "Resource usage summary:\n"
                + "\n"
                + "    CPU time :                                   0.01 sec.\n"
                + "    Max Memory :                                 -\n"
                + "    Average Memory :                             -\n"
                + "    Total Requested Memory :                     -\n"
                + "    Delta Memory :                               -\n"
                + "    Max Swap :                                   -\n"
                + "    Max Processes :                              -\n"
                + "    Max Threads :                                -\n"
                + "    Run time :                                   0 sec.\n"
                + "    Turnaround time :                            1 sec.\n"
                + "\n"
                + "The output (if any) follows:\n"
                + "\n"
                + "test\n"
                + "\n");
    assertThat(result.isSuccess()).isTrue();
  }

  @Test
  public void extractLsfJobResultNull() {
    assertThat(AbstractLsfExecutor.extractLsfJobResult(null)).isNull();
    assertThat(AbstractLsfExecutor.extractLsfJobResult("")).isNull();
    assertThat(AbstractLsfExecutor.extractLsfJobResult("invalid")).isNull();
  }
}
