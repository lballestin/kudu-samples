package org.kududb.examples.sample;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

/**
 *  Created by Luis B. on 16/11/16.
 */

public class CreateTable {

    private static final String KUDU_MASTER = System.getProperty(
            "kuduMaster", "localhost");

    public static void main(String[] args) {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "Tabla_LuisVM";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            List<ColumnSchema> columns = new ArrayList(3);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("description", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("key");
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}


