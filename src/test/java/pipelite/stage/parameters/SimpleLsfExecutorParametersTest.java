package pipelite.stage.parameters;

import org.junit.jupiter.api.Test;

public class SimpleLsfExecutorParametersTest {

  @Test
  public void validate() {
    new SimpleLsfExecutorParameters().validate();
    SimpleLsfExecutorParameters.builder().build().validate();
  }
}
