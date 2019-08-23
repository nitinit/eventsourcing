package com.test.casssandra;

import java.util.Optional;

import com.datastax.driver.core.Session;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;

public abstract class AbstractProcess extends AbstractLoggingActor {

    public static class Start {
        private final EventLogConfig config;
        private final Session session;
        private final int partition;

        public Start(EventLogConfig config, Session session, int partition) {
            this.config = config;
            this.session = session;
            this.partition = partition;
        }
    }

    private EventLogConfig config;
    private Session session;
    private ActorRef sender;

    protected EventLogConfig getConfig() {
        return config;
    }

    protected Session getSession() {
        return session;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Start.class, this::onStart)
                .build();
    }

    private void onStart(Start msg) {
        this.sender = sender();
        this.config = msg.config;
        this.session = msg.session;
        start(msg.partition);
    }

    protected abstract void start(int partition);

    protected void finished(Optional<Throwable> t) {
        sender.tell(new Ack(null, t), self());
    }

    protected int getEntitiesPerPartition() {
        return context().system().settings().config()
                .getInt("test.entitiesPerPartition");
    }

    protected int getEventsPerEntity() {
        return context().system().settings().config()
                .getInt("test.eventsPerEntity");
    }

}
