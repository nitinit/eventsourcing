package com.test.casssandra;

public class Event {

    private final String id;
    /**
     * The sequence number of the data in all events with the given id. The event log maintains a uniqueness constraint
     * on (id, sequence).
     * 
     * On recovery, data is returned in the order of the sequence number.
     * 
     * The event log itself does NOT put any further semantics on this (such as at which number it should start, that
     * there should be no gaps, etc).
     */
    private final long sequence;

    private final String data;

    @SuppressWarnings("unused")
    private Event() {
        this(null, 0L, null);
    }

    public Event(String id, long sequence, String data) {
        this.id = id;
        this.sequence = sequence;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public long getSequence() {
        return sequence;
    }

    public String getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        Event e = (Event) o;
        return e.getData().equals(this.getData()) &&
                e.getId().equals(this.getId()) &&
                e.getSequence() == this.sequence;
    }
}