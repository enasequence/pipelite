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
package pipelite.task.result;

public enum DefaultTaskExecutionResults implements TaskExecutionResult {
    SUCCESS(TaskExecutionResultType.SUCCESS, 0, null),
    PERMANENT_ERROR(TaskExecutionResultType.PERMANENT_ERROR, 1, Throwable.class);

    DefaultTaskExecutionResults(TaskExecutionResultType type, int exitCode, Class<? extends Throwable> cause) {
        this.type = type;
        this.exitCode = (byte) exitCode;
        this.cause = cause;
    }

    final TaskExecutionResultType type;
    final byte exitCode;
    final Class<? extends Throwable> cause;

    @Override
    public TaskExecutionResultType getExecutionResultType() {
        return type;
    }

    @Override
    public String getExecutionResult() {
        return name();
    }

    @Override
    public byte getExitCode() {
        return exitCode;
    }

    @Override
    public Class<? extends Throwable> getCause() {
        return cause;
    }
}
