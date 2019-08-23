package com.test.casssandra;

import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;

public class PersistentEntityManager extends AbstractProcess {

    public static Props props() {
        return Props.create(PersistentEntityManager.class);
    }

    private ActorRef eventLog;
    private int ackCount;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Terminated.class, this::onTerminated)
                .match(Ack.class, this::onAck)
                .build()
                .orElse(super.createReceive());
    }

    private void onTerminated(Terminated t) {
        if (t.actor().equals(eventLog)) {
            log().warning("EventLog terminated. Stopping self");
            finished(Optional.of(new RuntimeException("Unexpected eventlog termination")));
        }
    }

    private void onAck(Ack msg) {
        if (msg.getFailure().isPresent()) {
            log().warning("Client {} failed : {}", sender().path(), msg);
            finished(msg.getFailure());
        } else {
            ackCount--;
            if (ackCount == 0) {
                finished(Optional.empty());
            }
        }
    }

    @Override
    protected void start(int partition) {
        eventLog = context()
                .watch(context().actorOf(EventLog.props(getSession(), getConfig(), partition), "eventlog"));
        persistentEntityIds(partition).forEach(id -> {
            context().watch(context().actorOf(PersistentEntity.props(eventLog, getEventsPerEntity()), id));
            ackCount++;
        });
    }

    private Stream<String> persistentEntityIds(int partition) {
        return IntStream.range(0, getEntitiesPerPartition()).mapToObj(i -> "ent-" + partition + "-" + i);
    }
}
