package com.test.casssandra;

import java.util.Objects;
import java.util.Optional;

public class Ack {
    private final Object context;
    private final Optional<Throwable> failure;

    public Ack() {
        this(null);
    }

    public Ack(Object context) {
        this(context, (Throwable) null);
    }

    public Ack(Object context, Throwable t) {
        this(context, Optional.ofNullable(t));
    }

    public Ack(Object context, Optional<Throwable> failure) {
        this.context = context;
        this.failure = failure;
    }

    public Object getContext() {
        return context;
    }

    public Optional<Throwable> getFailure() {
        return failure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Ack ack = (Ack) o;
        return Objects.equals(context, ack.context) &&
                Objects.equals(failure, ack.failure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, failure);
    }

    @Override
    public String toString() {
        return "Ack{context=" + context + ", failure=" + failure + "}";
    }
}
