package com.test.casssandra;

import com.test.casssandra.EventLog.Insert;
import com.test.casssandra.EventLog.RecoveryMessage;
import com.test.casssandra.EventLog.Restore;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class PersistentEntity extends AbstractLoggingActor {

    public static Props props(ActorRef eventLog, int eventsPerEntity) {
        return Props.create(PersistentEntity.class, eventLog, eventsPerEntity);
    }

    private final ActorRef eventLog;
    private long nextSequence;
    private int eventsPerEntity;

    public PersistentEntity(ActorRef eventLog, int eventsPerEntity) {
        this.eventLog = eventLog;
        this.eventsPerEntity = eventsPerEntity;
        eventLog.tell(new Restore(self().path().name(), null), self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(RecoveryMessage.class, this::onRecoveryMessage).build();
    }

    private void onRecoveryMessage(RecoveryMessage msg) {
        log().info("method=onRecoveryMessage nextSequence={}", msg.getNextSequence());
        this.nextSequence = msg.getNextSequence();
        getContext().become(receiveBuilder().match(Ack.class, this::onAck).build());
        insertNextEvent();
    }

    private void onAck(Ack msg) {
        log().info("method=onAck ack={}", msg);
        if (msg.getFailure().isPresent()) {
            context().parent().tell(msg, self());
            context().stop(self());
        } else {
            insertNextEvent();
        }
    }

    private void insertNextEvent() {
        log().info("method=insertNextEvent nextSequence={}", nextSequence);
        if (nextSequence == eventsPerEntity) {
            context().parent().tell(new Ack(), self());
            context().stop(self());
        } else {
            eventLog.tell(new Insert(new Event(self().path().name(), nextSequence, "event-" + nextSequence), null),
                    self());
            nextSequence++;
        }
    }

}
