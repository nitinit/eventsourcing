package com.test.casssandra;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;
import scala.concurrent.Await;

import static java.lang.System.exit;

public class Main implements Closeable {

    private Cluster cluster;
    private Session session;
    private ActorSystem actorSystem;
    private EventLogConfig config;

    public Main() {
        config = new EventLogConfig("eventlog", "EventLog");
        connectToCluster();
        actorSystem = ActorSystem.create("CassandraTest");
    }

    public void write() throws Exception {
        createTable();
        ActorRef actorRef = actorSystem.actorOf(PersistentEntityManager.props(), "writeSide");

        PatternsCS.ask(actorRef, new AbstractProcess.Start(config, session, 0), Duration.ofMinutes(20))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    actorSystem.terminate();
                });

        Await.ready(actorSystem.whenTerminated(), scala.concurrent.duration.Duration.create(20, TimeUnit.MINUTES));
    }

    public void read() throws Exception {
        createTable();
        ActorRef actorRef = actorSystem.actorOf(ReadModelActor.props(), "readSide");
        PatternsCS.ask(actorRef, new AbstractProcess.Start(config, session, 0), Duration.ofMinutes(20))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    actorSystem.terminate();
                });
        Await.ready(actorSystem.whenTerminated(), scala.concurrent.duration.Duration.create(20, TimeUnit.MINUTES));
    }

    public void close() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private void connectToCluster() {
        cluster = Cluster.builder()
                .withClusterName("Test Cluster")
                .addContactPoint("127.0.0.1")
                .build();
        session = cluster.connect();
    }

    public static void main(String[] args) throws Exception {
        try (Main main = new Main()) {
            switch (args[0]) {
            case "clean":
                main.clean();
                break;
            case "write":
                main.write();
                break;
            case "read":
                main.read();
                break;
            }
        }
    }

    public void clean() {
        session.execute("DROP TABLE IF EXISTS " + config.getKeySpace() + "." + config.getTableName());
    }

    private void createTable() {
        session.execute("create table if not exists " + config.getKeySpace() + "." + config.getTableName() + " (" +
                "partition int," +
                "key bigint," +
                "id text," +
                "seq_nr bigint," +
                "data text," +
                "writerId int STATIC," +
                "PRIMARY KEY (partition, key)" +
                ")");

        session.execute("CREATE INDEX IF NOT EXISTS " + config.getTableName() + "_id ON " + config.getKeySpace() + "."
                + config.getTableName() + " ( id )");
    }
}
