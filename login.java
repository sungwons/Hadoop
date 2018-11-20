package useradmin;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class login {
    
    public static void main(String[] args) throws IOException{
        
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "User");
        
        String rowid = args[1];
        String password = args[2];
        String ip = args[3];
        
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY/MM/DD");
        long time = now.getTime();

        Put put = new Put(toBytes(rowid));

        if (password.equals("mypasswd")) {

            put.add(toBytes("Lastlogin"), toBytes("ip"), toBytes(ip));
            put.add(toBytes("Lastlogin"), toBytes("date"), toBytes(dateFormat.format(now)));
            put.add(toBytes("Lastlogin"), toBytes("time"), toBytes(time));
            put.add(toBytes("Lastlogin"), toBytes("success"), toBytes("success"));
            
            hTable.put(put);
            hTable.close();

        } else {

            System.out.println("Incorrect password.Try again.");
            System.exit(0);
        }
    
    }    
}
