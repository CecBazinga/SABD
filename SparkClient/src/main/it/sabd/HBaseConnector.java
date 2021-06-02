package it.sabd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseConnector {

    private static HBaseConnector instance = null;
    private Admin admin;
    private Connection conn;
    private Configuration conf;

    private HBaseConnector(){


        // Instantiating configuration class
        conf = HBaseConfiguration.create();

        try {
            conn = ConnectionFactory.createConnection(conf);
            admin  = conn.getAdmin();
        } catch (Exception e) { e.printStackTrace(); }

    }

    public static HBaseConnector getInstance(){

        if(instance == null) instance = new HBaseConnector();

        return instance;

    }

    public void run(){

        try {
            // Instantiating table descriptor class
            HTableDescriptor tableDescriptor = new
                    HTableDescriptor(TableName.valueOf("emp"));

            // Adding column families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor("personal"));
            tableDescriptor.addFamily(new HColumnDescriptor("professional"));

            // Execute the table through admin
            admin.createTable(tableDescriptor);
            System.out.println(" Table created ");

            Table htable = conn.getTable(TableName.valueOf("emp"));

            Put p = new Put(Bytes.toBytes("row1"));

            p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("colonna1"),Bytes.toBytes("ciao"));

            htable.put(p);

            /*
            Boolean b = admin.isTableDisabled(TableName.valueOf("emp"));
            if(!b){
                admin.disableTable(TableName.valueOf("emp"));
                admin.deleteTable(TableName.valueOf("emp"));
            }*/


        } catch(Exception e) { e.printStackTrace(); }

    }


}
