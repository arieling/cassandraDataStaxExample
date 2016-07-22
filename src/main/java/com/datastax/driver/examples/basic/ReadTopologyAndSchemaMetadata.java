package com.datastax.driver.examples.basic;

import com.datastax.driver.core.*;

public class ReadTopologyAndSchemaMetadata {

    static String[] CONTACT_POINTS = {"127.0.0.1"};
    static int PORT = 9042;

    public static void main(String[] args) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoints(CONTACT_POINTS).withPort(PORT)
                    .build();

            Metadata metadata = cluster.getMetadata();
            System.out.printf("Connected to cluster: %s%n", metadata.getClusterName());

            for (Host host : metadata.getAllHosts()) {
                System.out.printf("Datatacenter: %s; Host: %s; Rack: %s%n",
                        host.getDatacenter(), host.getAddress(), host.getRack());
            }

            for (KeyspaceMetadata keyspace : metadata.getKeyspaces()) {
                for (TableMetadata table : keyspace.getTables()) {
                    System.out.printf("Keyspace: %s; Table: %s%n",
                            keyspace.getName(), table.getName());
                }
            }

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
