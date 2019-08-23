package com.test.casssandra;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Optional;

import com.datastax.driver.core.Session;

import akka.actor.Props;

/**
 * Send {@link Insert} to insert message in the log. Reply will be a {@link Ack}. A {@link RecoveryMessage} will be sent
 * as reply to a {@link Restore} message.
 *
 * {@link HighestSequenceRequest} and {@link Replay} are needed to have the eventlog serve as akka persistence
 * EventLogJournal.
 */
public interface EventLog {

    static Props props(Session session, EventLogConfig config, int partition) {
        return Props.create(EventLogActor.class, session, config, partition);
    }

    class Insert {

        private final List<Event> events;
        private final Object context;

        public Insert() {
            this((Event) null, null);
        }

        public Insert(Event event, Object context) {

            this.events = singletonList(event);
            this.context = context;
        }

        public Insert(List<Event> events, Object context) {

            this.events = events;
            this.context = context;
        }

        public List<Event> getEvents() {
            return events;
        }

        public Object getContext() {
            return context;
        }
    }

    class Restore {

        private final String id;
        private final Optional<Long> fromSequence;
        private final Object context;

        public Restore() {
            this(null, null);
        }

        public Restore(String id, Object context) {

            this.id = id;
            this.fromSequence = Optional.empty();
            this.context = context;
        }

        public Restore(String id, Object context, long fromSequence) {

            this.id = id;
            this.fromSequence = Optional.of(fromSequence);
            this.context = context;
        }

        public String getId() {
            return id;
        }

        public Optional<Long> getFromSequence() {
            return fromSequence;
        }

        public Object getContext() {
            return context;
        }
    }

    class Replay {

        private final String id;
        private final Object context;
        private final long fromSequence;
        private final long toSequence;
        private final long max;

        @SuppressWarnings("unused")
        private Replay() {
            this(null, null, 0L, 0L, 0L);
        }

        public Replay(String id, Object context, long fromSequence, long toSequence, long max) {
            this.id = id;
            this.context = context;
            this.fromSequence = fromSequence;
            this.toSequence = toSequence;
            this.max = max;
        }

        public String getId() {
            return id;
        }

        public Object getContext() {
            return context;
        }

        public long getFromSequence() {
            return fromSequence;
        }

        public long getToSequence() {
            return toSequence;
        }

        public long getMax() {
            return max;
        }
    }

    class HighestSequenceRequest {

        private final String id;
        private final Object context;

        @SuppressWarnings("unused")
        private HighestSequenceRequest() {
            this(null, null);
        }

        public HighestSequenceRequest(String id, Object context) {
            this.id = id;
            this.context = context;
        }

        public String getId() {
            return id;
        }

        public Object getContext() {
            return context;
        }
    }

    class HighestSequenceReply {

        private final long sequence;
        private final Object context;

        @SuppressWarnings("unused")
        private HighestSequenceReply() {
            this(0L, null);
        }

        public HighestSequenceReply(long sequence, Object context) {
            this.sequence = sequence;
            this.context = context;
        }

        public long getSequence() {
            return sequence;
        }

        public Object getContext() {
            return context;
        }
    }

    class RecoveryMessage {

        private final List<String> data;
        private final long nextSequence;
        private final Object context;

        @SuppressWarnings("unused")
        private RecoveryMessage() {
            this(null, 0L, null);
        }

        public RecoveryMessage(List<String> data, long nextSequence, Object context) {

            this.data = data;
            this.nextSequence = nextSequence;
            this.context = context;
        }

        public List<String> getData() {
            return data;
        }

        public long getNextSequence() {
            return nextSequence;
        }

        public Object getContext() {
            return context;
        }
    }
}