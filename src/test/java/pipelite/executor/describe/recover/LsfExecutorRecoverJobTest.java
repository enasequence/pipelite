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
package pipelite.executor.describe.recover;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.executor.describe.recover.LsfExecutorRecoverJob.*;

import org.junit.jupiter.api.Test;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.stage.executor.StageExecutorResultAttribute;

public class LsfExecutorRecoverJobTest {

  private static LsfRequestContext request() {
    return new LsfRequestContext("validJobId", "validOutFile");
  }

  @Test
  public void testExtractExitCode() {
    assertThat(extractExitCode("Exited with exit code 1")).isEqualTo(1);
    assertThat(extractExitCode("Exited with exit code 3.")).isEqualTo(3);
    assertThat(extractExitCode("INVALID")).isNull();
  }

  @Test
  public void testGetRecoveryLines() {
    assertThat(getRecoveryLines("text\n" + "\n")).isEqualTo("text\n" + "\n");

    assertThat(
            getRecoveryLines(
                "text before\n"
                    + "\n"
                    + "The output (if any) follows:\n"
                    + "\n"
                    + "text after\n"
                    + "\n"))
        .isEqualTo("text before\n" + "\n");
  }

  @Test
  public void testExtractJobResultDoneSuccessfully() {
    LsfRequestContext request = request();
    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(
            request,
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
    assertThat(result.result.isSuccess()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
  }

  @Test
  public void testExtractJobResultCompletedSuccessfully() {
    LsfRequestContext request = request();
    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(
            request,
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
    assertThat(result.result.isSuccess()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
  }

  @Test
  public void testExtractJobResultCompletedWithExitCode() {
    LsfRequestContext request = request();
    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(
            request,
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
    assertThat(result.result.isExecutionError()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("127");
  }

  @Test
  public void testExtractJobResultExitedWithExitCode() {
    LsfRequestContext request = request();
    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(
            request,
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
    assertThat(result.result.isExecutionError()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("127");
  }

  @Test
  public void testExtractJobResultExitedWithTimeout() {
    LsfRequestContext request = request();
    DescribeJobsResult<LsfRequestContext> result =
        extractJobResult(
            request,
            "\n"
                + "ob <sleep 120> was submitted from host <codon-login-02> by user <rasko> in cluster <codon> at Wed Oct 26 18:09:37 2022\n"
                + "Job was executed on host(s) <hl-codon-113-03>, in queue <production>, as user <rasko> in cluster <codon> at Wed Oct 26 18:11:57 2022\n"
                + "</homes/rasko> was used as the home directory.\n"
                + "</homes/rasko> was used as the working directory.\n"
                + "Started at Wed Oct 26 18:11:57 2022\n"
                + "Terminated at Wed Oct 26 18:17:15 2022\n"
                + "Results reported at Wed Oct 26 18:17:15 2022\n"
                + " \n"
                + "Your job looked like:\n"
                + " \n"
                + "------------------------------------------------------------\n"
                + "# LSBATCH: User input\n"
                + "sleep 120\n"
                + "------------------------------------------------------------\n"
                + " \n"
                + "TERM_RUNLIMIT: job killed after reaching LSF run time limit.\n"
                + "Exited with signal termination: 12.\n"
                + " \n"
                + "Resource usage summary:\n"
                + " \n"
                + "    CPU time :                                   0.03 sec.\n"
                + "    Max Memory :                                 -\n"
                + "    Average Memory :                             -\n"
                + "    Total Requested Memory :                     -\n"
                + "    Delta Memory :                               -\n"
                + "    Max Swap :                                   -\n"
                + "    Max Processes :                              1\n"
                + "    Max Threads :                                1\n"
                + "    Run time :                                   338 sec.\n"
                + "    Turnaround time :                            458 sec.\n"
                + " \n"
                + "The output (if any) follows:\n"
                + "\n");
    assertThat(result.result.isTimeoutError()).isTrue();
    assertThat(result.result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isNull();
  }

  @Test
  public void extractJobResultInvalidInput() {
    LsfRequestContext request = request();
    assertThat(extractJobResult(request, null)).isNull();
    assertThat(extractJobResult(request, "")).isNull();
    assertThat(extractJobResult(request, "invalid")).isNull();
  }
}
