package com.test.casssandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

public class ReadModelActor extends AbstractProcess {

    public static Props props() {
        return Props.create(ReadModelActor.class);
    }

    private final static String LAUNCH_QUERY = "launchQuery";

    private final Map<String, Long> sequenceNumbers = new HashMap<>();
    private PreparedStatement queryByPosition;
    private long lastSeenKey;

    private int partition;

    @Override
    protected void start(int partition) {
        this.lastSeenKey = -1L;
        this.partition = partition;
        this.queryByPosition = getSession()
                .prepare(QueryBuilder
                        .select("id", "seq_nr", "key")
                  //      .select("id", "seq_nr", "key", "data")
                        .from(getConfig().getKeySpace(), getConfig().getTableName())
                        .where(eq("partition", QueryBuilder.bindMarker("partition")))
                        .and(gt("key", QueryBuilder.bindMarker("key")))
                        .orderBy(asc("key")))
                .setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        launchQuery();
    }

    private void launchQuery() {
        if (!stopIfFinished()) {
            log().info("method=launchQuery lastSeenKey={}", lastSeenKey);
            pipe(getSession().executeAsync(queryByPosition.bind(partition, lastSeenKey)));
        }
    }

    private void pipe(ListenableFuture<ResultSet> rsf) {
        ActorRef self = self();
        Futures.addCallback(rsf,
                new FutureCallback<ResultSet>() {

                    @Override
                    public void onFailure(Throwable t) {
                        self.tell(new Ack(null, t), self);
                    }

                    @Override
                    public void onSuccess(ResultSet rs) {
                        self.tell(new Ack(rs), self);
                    }
                });
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Ack.class, this::onAck)
                .matchEquals(LAUNCH_QUERY, this::onLaunchQuery)
                .build()
                .orElse(super.createReceive());
    }

    private void fail(Throwable t) {
        finished(Optional.of(t));
    }

    private void onAck(Ack msg) {
        if (msg.getFailure().isPresent()) {
            fail(msg.getFailure().get());
            return;
        }
        ResultSet rs = (ResultSet) msg.getContext();
        // How far we can go without triggering the blocking fetch:
        try {
            process(rs);
            boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
            if (wasLastPage) {
                if (!stopIfFinished()) {
                    log().info("method=onAck done iterating -- retrying in some time");
                    context().system().scheduler().scheduleOnce(Duration.apply(3, TimeUnit.SECONDS), self(),
                            LAUNCH_QUERY,
                            context().dispatcher(), self());
                }
            } else {
                log().info("method=onAck fetching more results");
                pipe(rs.fetchMoreResults());
            }
        } catch (Exception e) {
            fail(e);
        }
    }

    private void process(ResultSet rs) {
        int remainingInPage = rs.getAvailableWithoutFetching();
        for (Row row : rs) {
            sequenceNumbers.compute(row.getString(0), (id, seq_nr) -> {
                long key = row.getLong(2);
                if (key != lastSeenKey + 1) {
                    throw new RuntimeException("Expected key " + (lastSeenKey + 1) + " but got " + key);
                }
                lastSeenKey = key;
                long nextSeqNr = row.getLong(1);
                if (seq_nr != null) {
                    if (nextSeqNr != seq_nr + 1) {
                        throw new RuntimeException("Expected seq_nr " + (seq_nr + 1) + " but got " + nextSeqNr);
                    }
                }
                return nextSeqNr;
            });
            if (--remainingInPage == 0)
                break;
        }
    }

    private void onLaunchQuery(String msg) {
        launchQuery();
    }

    private boolean stopIfFinished() {

        if (sequenceNumbers.size() == getEntitiesPerPartition() &&
                sequenceNumbers.values().stream().min(Long::compareTo).map(l -> l == getEventsPerEntity() - 1)
                        .orElse(false)) {
            log().info("successfully seen all events -- stopping self");
            finished(Optional.empty());
            return true;
        }
        return false;
    }
}
