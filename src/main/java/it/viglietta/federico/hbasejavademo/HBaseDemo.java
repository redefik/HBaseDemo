package it.viglietta.federico.hbasejavademo;

import it.viglietta.federico.hbasejavademo.hbasebackend.HBaseClient;
import it.viglietta.federico.hbasejavademo.hbasebackend.exception.ConfigurationException;
import it.viglietta.federico.hbasejavademo.hbasebackend.exception.SchemaOperationException;

public class HBaseDemo {

    private static void schemaOperations(HBaseClient client) {
        try {
            System.out.println("Creating table DemoTable with column families: family1, family2, familyToBeDeleted...");
            client.createTable("DemoTable", "family1", "family2", "familyToBeDeleted");
            System.out.println("Table created");
            System.out.println();
            System.out.println("Adding column family family3...");
            client.addColumnFamily("DemoTable", "family3");
            System.out.println("Column family added");
            System.out.println("Deleting column family familyToBeDeleted...");
            client.deleteColumnFamily("DemoTable", "familyToBeDeleted");
            System.out.println("Column family deleted");
            System.out.println();
            System.out.println("Retrieving current schema...");
            client.printSchema("DemoTable");
            System.out.println();

            System.out.println("Deleting table DemoTable...");
            client.deleteTable("DemoTable");
            System.out.println("Table deleted");
        } catch (SchemaOperationException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("Configuring HBase...");
            HBaseClient client = HBaseClient.getInstance();
            System.out.println("Configuration completed");
            System.out.println();

            System.out.println("###################");
            System.out.println("#Schema Operations#");
            System.out.println("###################");
            System.out.println();

            schemaOperations(client);

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

}
