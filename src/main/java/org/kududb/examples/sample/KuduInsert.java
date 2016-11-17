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

public class KuduInsert {

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

            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            for (int i = 0; i < 6; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "value " + i);
                row.addString(2, "Probando Insert");
                session.apply(insert);
            }

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