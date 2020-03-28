package it.viglietta.federico.hbasejavademo.hbasebackend;

/*
* This class provides a set of methods used to interact with the HBase data store
* */

import it.viglietta.federico.hbasejavademo.hbasebackend.exception.ConfigurationException;
import it.viglietta.federico.hbasejavademo.hbasebackend.exception.DataOperationException;
import it.viglietta.federico.hbasejavademo.hbasebackend.exception.SchemaOperationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class HBaseClient {

    private static final String HBASE_CONFIGURATION_FILE = "/hbaseconf.properties";
    private static HBaseClient instance;
    private static Connection connection;

    private HBaseClient() {}

    /*
    * An HBaseClient object is a singleton. This method is used to instantiate una tantum the class.
    * It loads HBase configuration from hbaseconf.properties, validates the configuration and creates a Connection
    * object used later to interact with the data store. */
    public static synchronized HBaseClient getInstance() throws ConfigurationException {
        if (instance == null) {
            try {
                Properties properties = new Properties();
                properties.load(HBaseClient.class.getResourceAsStream(HBASE_CONFIGURATION_FILE));
                // TODO load Configuration in a more standard way
                Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", properties.getProperty("zookeeper_host"));
                configuration.set("hbase.zookeeper.property.clientPort", properties.getProperty("zookeeper_port"));
                configuration.set("hbase.master", properties.getProperty("master_address"));
                connection = ConnectionFactory.createConnection(configuration);
                instance = new HBaseClient();
            } catch (IOException e) {
                throw new ConfigurationException("An error occurred while configuring HBaseClient" , e.getCause());
            }
        }
        return instance;
    }

    /* Create a table with the given name and the provided column families*/
    public void createTable(String name, String ... families) throws SchemaOperationException {
        try (Admin admin = connection.getAdmin()){
            TableName tableName = TableName.valueOf(name);
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (String familyName : families) {
                HColumnDescriptor familyDescriptor = new HColumnDescriptor(familyName);
                tableDescriptor.addFamily(familyDescriptor);
            }
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            throw new SchemaOperationException("An error occurred during table creation", e.getCause());
        }
    }

    /* Drops the table with the given name*/
    public void deleteTable(String name) throws SchemaOperationException {
        try (Admin admin = connection.getAdmin()){
            TableName tableName = TableName.valueOf(name);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            throw new SchemaOperationException("An error occurred during table deletion", e.getCause());
        }
    }

    /* Add a column family to the table having the given name*/
    public void addColumnFamily(String name, String newFamily) throws SchemaOperationException {
        try (Admin admin = connection.getAdmin()){
            TableName tableName = TableName.valueOf(name);
            HColumnDescriptor familyDescriptor = new HColumnDescriptor(newFamily);
            admin.addColumn(tableName, familyDescriptor);
        } catch (IOException e) {
            throw new SchemaOperationException("An error occurred while adding a column family", e.getCause());
        }
    }

    /* Delete a column family from the table with the given name*/
    public void deleteColumnFamily(String name, String family) throws SchemaOperationException {
        try(Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(name);
            admin.deleteColumn(tableName, Bytes.toBytes(family));
        } catch (IOException e) {
            throw new SchemaOperationException("An error occurred while deleting a column family", e.getCause());
        }
    }

    /* Output the current schema of the given table*/
    public void printSchema(String table) throws SchemaOperationException {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(table));
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            System.out.println(table);
            System.out.println("Column families:");
            for (HColumnDescriptor familyDescriptor : columnFamilies) {
                System.out.println(familyDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            throw new SchemaOperationException("An error occurred while retrieving current schema", e.getCause());
        }
    }

    public void setColumns(String table, String rowKey, String columnFamily, String []columns, String[] values) throws DataOperationException {
        TableName tableName = TableName.valueOf(table);
        try (Table hTable = connection.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            if (columns.length != values.length) {
                throw new DataOperationException("Couldn't setting column values because columns and values array have not the same size");
            }
            for (int i = 0; i < columns.length; i++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            }
            hTable.put(put);
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while setting column values", e.getCause());
        }
    }

    public void scanTable(String table) throws DataOperationException {
        TableName tableName = TableName.valueOf(table);
        Scan scan = new Scan();
        try (Table hTable = connection.getTable(tableName); ResultScanner resultScanner = hTable.getScanner(scan))
        {
            for (Result result : resultScanner) {
                System.out.println("Found row:" + result);
            }
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while scanning the table", e.getCause());
        }
    }

    public void scanTableColumnFamily(String table, String columnFamily) throws DataOperationException {
        TableName tableName = TableName.valueOf(table);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        try (Table hTable = connection.getTable(tableName); ResultScanner resultScanner = hTable.getScanner(scan)) {
            for (Result result : resultScanner) {
                System.out.println(result);
            }
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while scanning the table", e.getCause());
        }
    }

    public void deleteColumns(String table, String rowKey, String family, String[] colToDelete) throws DataOperationException {
        TableName tableName = TableName.valueOf(table);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String column : colToDelete) {
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        }
        try (Table hTable = connection.getTable(tableName)) {
            hTable.delete(delete);
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while deleting columns", e.getCause());
        }
    }

    public void getRow(String table, String rowKey) throws DataOperationException {
        Get get = new Get(Bytes.toBytes(rowKey));
        TableName tableName = TableName.valueOf(table);
        try (Table hTable = connection.getTable(tableName)) {
            Result result = hTable.get(get);
            System.out.println(result);
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while getting an item", e.getCause());
        }
    }

    public void scanTableRowKeyPrefix(String table, String prefix) throws DataOperationException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(prefix));
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
        TableName tableName = TableName.valueOf(table);
        try (Table hTable = connection.getTable(tableName); ResultScanner resultScanner = hTable.getScanner(scan)) {
            for (Result result : resultScanner) {
                System.out.println("Found row: " + result);
            }
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while scanning the table", e.getCause());
        }
    }

    public void getUsersBornIn1993() throws DataOperationException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("a"));
        scan.setRowPrefixFilter(Bytes.toBytes("a"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("born"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1993"));
        scan.setFilter(filter);
        TableName tableName = TableName.valueOf("Users");
        try (Table hTable = connection.getTable(tableName); ResultScanner resultScanner = hTable.getScanner(scan)) {
            for (Result result : resultScanner) {
                System.out.println("Found row: " + result);
            }
        } catch (IOException e) {
            throw new DataOperationException("An error occurred while scanning the table", e.getCause());
        }
    }
}
