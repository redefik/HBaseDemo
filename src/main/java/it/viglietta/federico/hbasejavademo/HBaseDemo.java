package it.viglietta.federico.hbasejavademo;

import it.viglietta.federico.hbasejavademo.hbasebackend.HBaseClient;
import it.viglietta.federico.hbasejavademo.hbasebackend.exception.ConfigurationException;
import it.viglietta.federico.hbasejavademo.hbasebackend.exception.DataOperationException;
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

    private static void dataOperations(HBaseClient client) {
        try {
            System.out.println("Creating a table Customers with families: profile and orders...");
            client.createTable("Customers", "profile", "orders");
            System.out.println("Table created");
            System.out.println();
            System.out.println("Inserting two customes with profile information...");
            String[] columnsToSet = {"name", "billingAddress", "payment"};
            String[] values1 = {"pippo", "Parco della Vittoria, 3", "VISA"};
            client.setColumns("Customers", "u1", "profile", columnsToSet, values1);
            String[] values2 = {"pluto", "Via Marco Polo, 4", "American Express"};
            client.setColumns("Customers", "u2", "profile", columnsToSet, values2);
            System.out.println("Customers inserted");
            System.out.println("Table scanning...");
            client.scanTable("Customers");
            System.out.println();
            System.out.println("HBase is sparse: Adding orders x1, x2 to pippo and orders y1 to pluto...");
            String[] columns1 = {"x1", "x2"};
            String[] columns2 = {"y1"};
            String[] colValues1 = {"x1Data", "x2Data"};
            String[] colValues2 = {"y1Data"};
            client.setColumns("Customers", "u1", "orders", columns1, colValues1);
            client.setColumns("Customers", "u2", "orders", columns2, colValues2);
            System.out.println("Columns added");
            System.out.println("Retrieving customers and orders...");
            client.scanTableColumnFamily("Customers", "orders");
            System.out.println();
            System.out.println("Setting x1 order of pippo to new value");
            String[] columnsToModify = {"x1"};
            String[] newValue = {"x1NewData"};
            client.setColumns("Customers", "u1", "orders", columnsToModify, newValue);
            System.out.println("Scanning table...(Note different timestamp for x1)");
            client.scanTable("Customers");
            System.out.println();
            System.out.println("Deleting order x1...");
            String[] colToDelete = {"x1"};
            client.deleteColumns("Customers", "u1", "orders", colToDelete);
            System.out.println("Deleted order");
            System.out.println("Since HBase is multi-version you should see the previously order. Scanning table...");
            client.scanTable("Customers");
            System.out.println();
            System.out.println("Retrieving u1...");
            client.getRow("Customers", "u1");
            System.out.println();
            System.out.println("Deleting table Customers...");
            client.deleteTable("Customers");
            System.out.println("Table deleted");

        } catch (SchemaOperationException | DataOperationException e) {
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

            System.out.println("###################");
            System.out.println("#Data Operations#");
            System.out.println("###################");
            System.out.println();
            dataOperations(client);

        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

}
