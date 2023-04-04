/*
 * Copyright 2021 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.sdk.workflow.executor.task;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.sdk.workflow.def.tasks.DynamicFork;
import com.netflix.conductor.sdk.workflow.def.tasks.DynamicForkInput;
import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.OutputParam;
import com.netflix.conductor.sdk.workflow.utils.ObjectMapperProvider;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

public class AnnotatedWorker2 implements Worker {

    private String name;

    private FunctionExecutor executor;

    private Class<?>[] parameterTypes;

    private ObjectMapper om = new ObjectMapperProvider().getObjectMapper();

    private int pollingInterval = 100;

    private Set<TaskResult.Status> failedStatuses =
            Set.of(TaskResult.Status.FAILED, TaskResult.Status.FAILED_WITH_TERMINAL_ERROR);

    public AnnotatedWorker2(String name, FunctionExecutor executor, Class<?>[] parameterTypes) {
        this.name = name;
        this.executor = executor;
        this.parameterTypes = parameterTypes;
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public String getTaskDefName() {
        return name;
    }

    @Override
    public TaskResult execute(Task task) {
        TaskResult result = null;
        try {
            TaskContext context = TaskContext.set(task);
            Object[] parameters = getInvocationParameters(task);
            Object invocationResult = executor.execute(parameters);
            result = setValue(invocationResult, context.getTaskResult());
            if (!failedStatuses.contains(result.getStatus())
                    && result.getCallbackAfterSeconds() > 0) {
                result.setStatus(TaskResult.Status.IN_PROGRESS);
            }
        } catch (Exception invocationTargetException) {
            if (result == null) {
                result = new TaskResult(task);
            }
            Throwable e = invocationTargetException.getCause();
            e.printStackTrace();
            if (e instanceof NonRetryableException) {
                result.setStatus(TaskResult.Status.FAILED_WITH_TERMINAL_ERROR);
            } else {
                result.setStatus(TaskResult.Status.FAILED);
            }

            result.setReasonForIncompletion(e.getMessage());
            StringBuilder stackTrace = new StringBuilder();
            for (StackTraceElement stackTraceElement : e.getStackTrace()) {
                String className = stackTraceElement.getClassName();
                if (className.startsWith("jdk.")
                        || className.startsWith(AnnotatedWorker2.class.getName())) {
                    break;
                }
                stackTrace.append(stackTraceElement);
                stackTrace.append("\n");
            }
            result.log(stackTrace.toString());
        }

        return result;
    }

    private Object[] getInvocationParameters(Task task) {

        if (parameterTypes.length == 1 && parameterTypes[0].equals(Task.class)) {
            return new Object[] {task};
        } else if (parameterTypes.length == 1 && parameterTypes[0].equals(Map.class)) {
            return new Object[] {task.getInputData()};
        }

        return getParameters(task);
    }

    private Object[] getParameters(Task task) {
        Object[] values = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Object value = task.getInputData().get("arg" + i);
            if(value == null) {
                value = task.getInputData();
            }
            values[i] = om.convertValue(value, parameterTypes[i]);
        }

        return values;
    }
    private TaskResult setValue(Object invocationResult, TaskResult result) {

        if (invocationResult == null) {
            result.setStatus(TaskResult.Status.COMPLETED);
            return result;
        }


        if (invocationResult instanceof TaskResult) {

            return (TaskResult) invocationResult;

        } else if (invocationResult instanceof Map) {
            Map resultAsMap = (Map) invocationResult;
            result.getOutputData().putAll(resultAsMap);
            result.setStatus(TaskResult.Status.COMPLETED);
            return result;
        } else if (invocationResult instanceof String
                || invocationResult instanceof Number
                || invocationResult instanceof Boolean) {
            result.getOutputData().put("result", invocationResult);
            result.setStatus(TaskResult.Status.COMPLETED);
            return result;
        } else if (invocationResult instanceof List) {

            List resultAsList = om.convertValue(invocationResult, List.class);
            result.getOutputData().put("result", resultAsList);
            result.setStatus(TaskResult.Status.COMPLETED);
            return result;

        } else if (invocationResult instanceof DynamicForkInput) {
            DynamicForkInput forkInput = (DynamicForkInput) invocationResult;
            List<com.netflix.conductor.sdk.workflow.def.tasks.Task<?>> tasks = forkInput.getTasks();
            List<WorkflowTask> workflowTasks = new ArrayList<>();
            for (com.netflix.conductor.sdk.workflow.def.tasks.Task<?> sdkTask : tasks) {
                workflowTasks.addAll(sdkTask.getWorkflowDefTasks());
            }
            result.getOutputData().put(DynamicFork.FORK_TASK_PARAM, workflowTasks);
            result.getOutputData().put(DynamicFork.FORK_TASK_INPUT_PARAM, forkInput.getInputs());
            result.setStatus(TaskResult.Status.COMPLETED);
            return result;

        } else {
            Map resultAsMap = om.convertValue(invocationResult, Map.class);
            result.getOutputData().putAll(resultAsMap);
            result.setStatus(TaskResult.Status.COMPLETED);
            return result;
        }
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    @Override
    public int getPollingInterval() {
        return pollingInterval;
    }
}
