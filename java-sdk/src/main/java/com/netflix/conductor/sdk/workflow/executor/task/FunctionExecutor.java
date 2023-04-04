package com.netflix.conductor.sdk.workflow.executor.task;

import java.lang.reflect.Method;
import java.util.function.Function;

public class FunctionExecutor {

    private final Function<Object[], Object> function;

    public Method method;

    public FunctionExecutor(Function<Object[], Object> function) {
        this.function = function;
        try {
            this.method = FunctionExecutor.class.getDeclaredMethod("execute", new Object[0].getClass());
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public Object execute(Object...args) {
        return function.apply(args);
    }
}
