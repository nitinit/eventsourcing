package com.test.casssandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.japi.Pair;

public class EventLogActor extends AbstractLoggingActor implements EventLog {

    private final int writerId;
    private final Session session;
    private final int partition;
    private final EventLogConfig config;
    private final PreparedStatement insertStatement;
    private final PreparedStatement restoreStatement;
    private final PreparedStatement updateWriterIdStatement;

    private final PreparedStatement controlStatement;

    private long lastUsedKey;
    private List<Pair<Insert, ActorRef>> toInsert;
    private List<Pair<Insert, ActorRef>> inserting;


    public EventLogActor(Session session, EventLogConfig config, int partition) {
        this.session = session;
        this.partition = partition;
        this.config = config;
        this.toInsert = new ArrayList<>();

        insertStatement = session.prepare(QueryBuilder.insertInto(config.getKeySpace(), config.getTableName())
                .value("writerId", QueryBuilder.bindMarker("writerId"))
                .value("partition", QueryBuilder.bindMarker("partition"))
                .value("key", QueryBuilder.bindMarker("key"))
                .value("id", QueryBuilder.bindMarker("id"))
                .value("seq_nr", QueryBuilder.bindMarker("seq_nr"))
                .value("data", QueryBuilder.bindMarker("data")))
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        updateWriterIdStatement = session.prepare(QueryBuilder.update(config.getKeySpace(), config.getTableName())
                .with(set("writerId", QueryBuilder.bindMarker("newWriterId")))
                .where(eq("partition", QueryBuilder.bindMarker("partition")))
                .onlyIf(eq("writerId", QueryBuilder.bindMarker("existingWriterId"))))
                .setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        restoreStatement = session
                .prepare(QueryBuilder.select("data", "seq_nr").from(config.getKeySpace(), config.getTableName())
                        .where(eq("partition", QueryBuilder.bindMarker("partition")))
                        .and(eq("id", QueryBuilder.bindMarker("id"))))
                // TODO verify that there is no need to add an ORDER BY key here
                .setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        controlStatement = session
                .prepare(QueryBuilder.select("data", "seq_nr").from(config.getKeySpace(), config.getTableName())
                        .where(eq("partition", QueryBuilder.bindMarker("partition")))
                        .and(eq("id", QueryBuilder.bindMarker("id"))))
                // TODO verify that there is no need to add an ORDER BY key here
                .setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

        this.writerId = initializeWriterId();
        this.lastUsedKey = initializeKey();
        log().info("Initialized EL partition={}, writerId={}, lastUsedKey={}", partition, writerId, lastUsedKey);

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Insert.class, this::onInsert)
                .match(Ack.class, this::onAck)
                .match(Restore.class, this::onRestore)
                .build();
    }

    @Override
    public void postStop() throws Exception {
        log().warning("method=postStop");
        super.postStop();
    }

    private void onInsert(Insert msg) {
        toInsert.add(Pair.create(msg, sender()));
        log().info("method=onInsert toInsertSize={}", toInsert.size());
        if (inserting != null) {
            return;
        }
        startInserting();
    }

    private void startInserting() {
        log().info("method=startInserting batchSize={}", toInsert.size());
        inserting = toInsert;
        toInsert = new ArrayList<>();
        BatchStatement bs = new BatchStatement(Type.LOGGED);
        bs.add(updateWriterIdStatement.bind(writerId, partition, writerId));
        inserting.forEach(p -> add(bs, p.first()));
        ActorRef self = self();
        Futures.addCallback(session.executeAsync(bs), new FutureCallback<ResultSet>() {
            public void onSuccess(ResultSet rs) {
                self.tell(new Ack(rs), self);
            }

            public void onFailure(Throwable t) {
                self.tell(new Ack(null, t), self);
            }
        });
    }

    private void onAck(Ack msg) {
        log().info("method=onAck ack={}", msg);
        if (msg.getFailure().isPresent()) {
            log().error(msg.getFailure().get(), "Failed to insert");
            throw new IllegalStateException("I'm obsolete!");
        }
        inserting.forEach(p -> p.second().tell(new Ack(p.first().getContext()), self()));
        inserting = null;
        if (!toInsert.isEmpty()) {
            startInserting();
        }
    }

    private void onRestore(Restore msg) {
        log().info("method=onRestore id={}", msg.getId());
        ActorRef self = self();
        ActorRef sender = sender();
        Futures.addCallback(session.executeAsync(restoreStatement.bind(partition, msg.getId())),
                new FutureCallback<ResultSet>() {
                    public void onSuccess(ResultSet rs) {
                        List<String> data = new ArrayList<>();
                        long seq_nr = 0;
                        for (Row r : rs) {
                            data.add(r.getString(0));
                            seq_nr = r.getLong(1);
                        }
                        sender.tell(new RecoveryMessage(data, seq_nr, msg.getContext()), self);
                    }

                    public void onFailure(Throwable t) {
                        self.tell(new Ack(null, t), self);
                    }
                });
    }

    private void add(BatchStatement bs, Insert insert) {
//            insert.getEvents()
//                    .forEach(e -> bs.add(insertStatement.bind(writerId, partition, ++lastUsedKey, e.getId(),
//                            e.getSequence(), e.getData())))
        boolean check = false;
        for (Event e :insert.getEvents()) {
            ResultSet rs = session.execute(controlStatement.bind(partition, e.getId()));
            for (Row r : rs) {
                if ( (r.getString(0).equals(e.getData())) &&
                        (r.getLong(1) == e.getSequence())) {
                    check = true;
                    break;
                }
            }
            if (!check)
                bs.add(insertStatement.bind(writerId, partition, ++lastUsedKey, e.getId(), e.getSequence(), e.getData()));
        }
    }

    private int initializeWriterId() {
        int result = 0;
        Integer existingWriterId = null;
        ResultSet rs = session.execute(QueryBuilder.select("writerId").from(config.getKeySpace(), config.getTableName())
                .where(eq("partition", partition)).setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL));
        for (Row r : rs) {
            existingWriterId = r.getInt(0);
            result = existingWriterId + 1;
        }
        Statement setWriterId = updateWriterIdStatement.bind(result, partition, existingWriterId);
        if (!session.execute(setWriterId).wasApplied()) {
            throw new IllegalStateException("I'm obsolete!");
        }
        return result;
    }

    private long initializeKey() {
        ResultSet rs = session
                .execute(QueryBuilder.select("key").from(config.getKeySpace(), config.getTableName())
                        .where(eq("partition", partition))
                        .orderBy(QueryBuilder.desc("key"))
                        .limit(1)
                        .setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL));
        Row row = rs.one();
        if (row.isNull(0)) {
            return -1L;
        }
        return row.getLong(0);
    }

}
