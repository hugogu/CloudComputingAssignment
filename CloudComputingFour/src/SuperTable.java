import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable {

    public static void main(String[] args) throws IOException {
        final Configuration con = HBaseConfiguration.create();
        final HBaseAdmin admin = new HBaseAdmin(con);
        final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));
        admin.createTable(tableDescriptor);

        final HTable table = new HTable(con, "powers");
        addRow(table, "row1", "superman", "strength", "clark", "100");
        addRow(table, "row2", "batman", "money", "bruce", "50");
        addRow(table, "row3", "wolverine", "healing", "logan", "75");
        table.close();

        final Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
        final ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    private static void addRow(HTable table, String row, String hero, String power, String name, String xp) throws IOException {
        final Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes(hero));
        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes(power));
        put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(xp));
        table.put(put);
    }
}
