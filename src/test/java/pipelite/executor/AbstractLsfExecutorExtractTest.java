/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.LsfRequestContext;
import pipelite.stage.executor.StageExecutorResult;

public class AbstractLsfExecutorExtractTest {

  @Test
  public void extractJobIdFromBsubOutput() {
    assertThat(
            AbstractLsfExecutor.extractJobIdFromBsubOutput(
                "Job <2848143> is submitted to default queue <research-rh74>."))
        .isEqualTo("2848143");
    assertThat(AbstractLsfExecutor.extractJobIdFromBsubOutput("Job <2848143> is submitted "))
        .isEqualTo("2848143");
    assertThrows(
        PipeliteException.class, () -> AbstractLsfExecutor.extractJobIdFromBsubOutput("INVALID"));
  }

  @Test
  public void extractNotFoundJobIdFromBjobsOutput() {
    assertThat(
            AbstractLsfExecutor.extractNotFoundJobIdFromBjobsOutput("Job <345654> is not found."))
        .isEqualTo("345654");
    assertThat(AbstractLsfExecutor.extractNotFoundJobIdFromBjobsOutput("Job <345654> is not found"))
        .isEqualTo("345654");
    assertThat(AbstractLsfExecutor.extractNotFoundJobIdFromBjobsOutput("Job <345654> is "))
        .isNull();
    assertThat(AbstractLsfExecutor.extractNotFoundJobIdFromBjobsOutput("INVALID")).isNull();
  }

  @Test
  public void extractExitCodeFromOutFile() {
    assertThat(AbstractLsfExecutor.extractExitCodeFromOutFile("Exited with exit code 1"))
        .isEqualTo(1);
    assertThat(AbstractLsfExecutor.extractExitCodeFromOutFile("Exited with exit code 3."))
        .isEqualTo(3);
    assertThat(AbstractLsfExecutor.extractExitCodeFromOutFile("INVALID")).isNull();
  }

  @Test
  public void extractJobResultFromBjobsOutputPending() {
    LsfRequestContext requestContext = new LsfRequestContext("861487", "outFile");
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(List.of(requestContext));
    DescribeJobsResult<LsfRequestContext> result =
        AbstractLsfExecutor.extractJobResultFromBjobsOutput("861487|PEND|-|-|-|-|-\n", requests);
    assertThat(result.request.getJobId()).isEqualTo("861487");
    assertThat(result.result.isActive()).isTrue();
  }

  @Test
  public void extractResultsFromBjobsOutputPending() {
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(
            Arrays.asList(
                new LsfRequestContext("861487", "outFile"),
                new LsfRequestContext("861488", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        AbstractLsfExecutor.extractJobResultsFromBjobsOutput(
            "861487|PEND|-|-|-|-|-\n" + "861488|PEND|-|-|-|-|-\n", requests);

    assertThat(results.found.size()).isEqualTo(2);
    assertThat(results.notFound.size()).isEqualTo(0);
    assertThat(results.found.get(0).request.getJobId()).isEqualTo("861487");
    assertThat(results.found.get(1).request.getJobId()).isEqualTo("861488");
    assertThat(results.found.get(0).result.isActive()).isTrue();
    assertThat(results.found.get(1).result.isActive()).isTrue();
  }

  @Test
  public void extractResultsFromBjobsOutputDone() {
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(
            Arrays.asList(
                new LsfRequestContext("872793", "outFile"),
                new LsfRequestContext("872794", "outFile"),
                new LsfRequestContext("872795", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        AbstractLsfExecutor.extractJobResultsFromBjobsOutput(
            "872793|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872794|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n"
                + "872795|DONE|-|0.0 second(s)|-|-|hx-noah-05-14\n",
            requests);

    assertThat(results.found.size()).isEqualTo(3);
    assertThat(results.found.get(0).request.getJobId()).isEqualTo("872793");
    assertThat(results.found.get(1).request.getJobId()).isEqualTo("872794");
    assertThat(results.found.get(2).request.getJobId()).isEqualTo("872795");
    assertThat(results.found.get(0).result.isSuccess()).isTrue();
    assertThat(results.found.get(1).result.isSuccess()).isTrue();
    assertThat(results.found.get(2).result.isSuccess()).isTrue();
  }

  @Test
  public void extractResultsFromBjobsExitAndNotFound() {
    DescribeJobsPollRequests<LsfRequestContext> requests =
        new DescribeJobsPollRequests<>(
            Arrays.asList(
                new LsfRequestContext("873206", "outFile"),
                new LsfRequestContext("873207", "outFile"),
                new LsfRequestContext("6065212", "outFile"),
                new LsfRequestContext("873209", "outFile")));

    DescribeJobsResults<LsfRequestContext> results =
        AbstractLsfExecutor.extractJobResultsFromBjobsOutput(
            "873206|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "873207|EXIT|127|0.0 second(s)|-|-|hx-noah-43-02\n"
                + "Job <6065212> is not found\n"
                + "873209|EXIT|127|0.0 second(s)|-|-|hx-noah-10-04\n",
            requests);

    assertThat(results.found.size()).isEqualTo(3);
    assertThat(results.notFound.size()).isEqualTo(1);
    assertThat(results.found.get(0).request.getJobId()).isEqualTo("873206");
    assertThat(results.found.get(1).request.getJobId()).isEqualTo("873207");
    assertThat(results.notFound.get(0).getJobId()).isEqualTo("6065212");
    assertThat(results.found.get(2).request.getJobId()).isEqualTo("873209");
    assertThat(results.found.get(0).result.isError()).isTrue();
    assertThat(results.found.get(1).result.isError()).isTrue();
    assertThat(results.found.get(2).result.isError()).isTrue();
  }

  @Test
  public void extractResultFromBhistError() {
    StageExecutorResult result =
        AbstractLsfExecutor.recoverJobUsingOutFile(
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
  public void extractResultFromBhistSuccess() {
    StageExecutorResult result =
        AbstractLsfExecutor.recoverJobUsingOutFile(
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
  public void extractResultFromOutFileError() {
    StageExecutorResult result =
        AbstractLsfExecutor.recoverJobUsingOutFile(
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
  public void extractResultFromOutFileSuccess() {
    StageExecutorResult result =
        AbstractLsfExecutor.recoverJobUsingOutFile(
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
  public void extractResultFromBhistOutputOrOutFile() {
    assertThat(AbstractLsfExecutor.recoverJobUsingOutFile(null)).isNull();
    assertThat(AbstractLsfExecutor.recoverJobUsingOutFile("")).isNull();
    assertThat(AbstractLsfExecutor.recoverJobUsingOutFile("invalid")).isNull();
  }
}
