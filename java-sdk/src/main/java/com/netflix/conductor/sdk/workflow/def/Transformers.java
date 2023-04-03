package com.netflix.conductor.sdk.workflow.def;

public class Transformers {


    @FunctionalInterface
    public interface Transformer1<T1, R> {
        public R apply(T1 arg);
    }

    @FunctionalInterface
    public interface Transformer2<T1, T2, R> {
        public R apply(T1 arg1, T2 arg2);
    }

    @FunctionalInterface
    public interface Transformer3<T1, T2, T3, R> {
        public R apply(T1 arg1, T2 arg2, T3 arg3);
    }
}
