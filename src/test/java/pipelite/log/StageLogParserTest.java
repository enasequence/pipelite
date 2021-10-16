package pipelite.log;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StageLogParserTest {

  @Test
  public void withoutError() {
    assertThat(new StageLogParser("TEST").getError()).isNull();
    assertThat(new StageLogParser("TEST\nTEST").getError()).isNull();
    assertThat(new StageLogParser("TEST\nTEST\nTEST").getError()).isNull();
    assertThat(new StageLogParser("TEST\nTEST\nTEST\n").getError()).isNull();
    assertThat(new StageLogParser("TEST [PIPELITE_ERROR]").getError()).isNull();
  }

  @Test
  public void withError() {
    assertThat(new StageLogParser("{{PIPELITE_ERROR: duh}}").getError()).isEqualTo("duh");
    assertThat(new StageLogParser(" {{ PIPELITE_ERROR: duh }}").getError()).isEqualTo("duh");
    assertThat(new StageLogParser("TEST{{ PIPELITE_ERROR: duh }}TEST").getError()).isEqualTo("duh");
    assertThat(
            new StageLogParser("\\nTEST\\nTEST\\n{{ PIPELITE_ERROR: duh duh }}\nTEST\nTEST\n")
                .getError())
        .isEqualTo("duh duh");
  }

  @Test
  public void withJavaException() {
    // Test single exception parsing.
    assertThat(
            new StageLogParser(
                    "TEST\nTEST\nTEST\n TEST: TEST : pipelite.exception.PipeliteException: Failed to execute ssh call: mkdir -p /nfs/panda/production/ena/flow/log/pipelite/embl_load_cram_ref_9/1\n"
                        + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n"
                        + "\tat java.lang.Thread.run(Thread.java:748)\n"
                        + "Caused by: org.apache.sshd.common.SshException: No more authentication methods available: TEST\n"
                        + "\tat sun.nio.ch.Invoker$2.run(Invoker.java:218)\n"
                        + "\tat sun.nio.ch.AsynchronousChannelGroupImpl$1.run(AsynchronousChannelGroupImpl.java:112)\n"
                        + "\t... 3 more\n")
                .getTrace())
        .isEqualTo(
            "pipelite.exception.PipeliteException: Failed to execute ssh call: mkdir -p /nfs/panda/production/ena/flow/log/pipelite/embl_load_cram_ref_9/1\n"
                + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n"
                + "\tat java.lang.Thread.run(Thread.java:748)\n"
                + "Caused by: org.apache.sshd.common.SshException: No more authentication methods available: TEST\n"
                + "\tat sun.nio.ch.Invoker$2.run(Invoker.java:218)\n"
                + "\tat sun.nio.ch.AsynchronousChannelGroupImpl$1.run(AsynchronousChannelGroupImpl.java:112)\n"
                + "\t... 3 more\n");
    // Test single exception parsing.
    assertThat(
            new StageLogParser(
                    "TEST\nTEST\nTEST\n TEST: TEST : pipelite.exception.PipeliteException: Failed to execute ssh call: mkdir -p /nfs/panda/production/ena/flow/log/pipelite/embl_load_cram_ref_9/1\n"
                        + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n"
                        + "TEST TEST\n")
                .getTrace())
        .isEqualTo(
            "pipelite.exception.PipeliteException: Failed to execute ssh call: mkdir -p /nfs/panda/production/ena/flow/log/pipelite/embl_load_cram_ref_9/1\n"
                + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n");
    // Test multiple exception parsing.
    assertThat(
            new StageLogParser(
                    "pipelite.exception.PipeliteException: TEST\n"
                        + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n"
                        + "\tat java.lang.Thread.run(Thread.java:748)\n"
                        + "Caused by: org.apache.sshd.common.SshException: No more authentication methods available: TEST\n"
                        + "\tat sun.nio.ch.Invoker$2.run(Invoker.java:218)\n"
                        + "\tat sun.nio.ch.AsynchronousChannelGroupImpl$1.run(AsynchronousChannelGroupImpl.java:112)\n"
                        + "\t... 3 more\n"
                        + "TEST\nTEST\nTEST\n TEST: TEST : pipelite.exception.PipeliteException: Failed to execute ssh call: mkdir -p /nfs/panda/production/ena/flow/log/pipelite/embl_load_cram_ref_9/1\n"
                        + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n"
                        + "\tat java.lang.Thread.run(Thread.java:748)\n"
                        + "Caused by: org.apache.sshd.common.SshException: No more authentication methods available: TEST\n"
                        + "\tat sun.nio.ch.Invoker$2.run(Invoker.java:218)\n"
                        + "\tat sun.nio.ch.AsynchronousChannelGroupImpl$1.run(AsynchronousChannelGroupImpl.java:112)\n"
                        + "\t... 3 more\n")
                .getTrace())
        .isEqualTo(
            "pipelite.exception.PipeliteException: Failed to execute ssh call: mkdir -p /nfs/panda/production/ena/flow/log/pipelite/embl_load_cram_ref_9/1\n"
                + "\tat pipelite.executor.cmd.SshCmdRunner.execute(SshCmdRunner.java:84)\n"
                + "\tat java.lang.Thread.run(Thread.java:748)\n"
                + "Caused by: org.apache.sshd.common.SshException: No more authentication methods available: TEST\n"
                + "\tat sun.nio.ch.Invoker$2.run(Invoker.java:218)\n"
                + "\tat sun.nio.ch.AsynchronousChannelGroupImpl$1.run(AsynchronousChannelGroupImpl.java:112)\n"
                + "\t... 3 more\n");
  }
}
