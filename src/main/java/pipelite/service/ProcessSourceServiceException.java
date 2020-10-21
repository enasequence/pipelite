package pipelite.service;

public class ProcessSourceServiceException extends RuntimeException {
  public ProcessSourceServiceException(String message) {
    super(message);
  }

  public ProcessSourceServiceException(String message, Throwable cause) {
    super(message, cause);
  }
}
