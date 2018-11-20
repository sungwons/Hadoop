package useradmin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class listall {
    
    public static void main(String[] args) throws IOException{
        
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "User");
        
        String scanTable = args[0];
               
        Scan scan = new Scan();
        
        ResultScanner scanner = hTable.getScanner(scan);
        
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()){
            print(rr);
        }
        
    hTable.close();
            
    }

    private static void print(Result rr) {
        System.out.println("-------------------------------------");
        System.out.println("RowId: " + Bytes.toString(rr.getRow()));
        
        byte[] val1 = rr.getValue(toBytes("Creds"), toBytes("email"));
        System.out.println("Creds:email: " + Bytes.toString(val1));
        
        byte[] val2 = rr.getValue(toBytes("Creds"), toBytes("password"));
        System.out.println("Creds:password: " + Bytes.toString(val2));
        
        byte[] val3 = rr.getValue(toBytes("Prefs"), toBytes("status"));
        System.out.println("Prefs:status: " + Bytes.toString(val3));
        
        byte[] val4 = rr.getValue(toBytes("Prefs"), toBytes("date_of_birth"));
        System.out.println("Prefs:date_of_birth: " + Bytes.toString(val4));
        
        byte[] val5 = rr.getValue(toBytes("Prefs"), toBytes("security_question"));
        System.out.println("Prefs:security_question: " + Bytes.toString(val5));
        
        byte[] val6 = rr.getValue(toBytes("Prefs"), toBytes("security_answer"));
        System.out.println("Prefs:security_answer: " + Bytes.toString(val6));
        
        byte[] val7 = rr.getValue(toBytes("Lastlogin"), toBytes("ip"));
        System.out.println("Lastlogin:ip: " + Bytes.toString(val7));
        
        byte[] val8 = rr.getValue(toBytes("Lastlogin"), toBytes("date"));
        System.out.println("Lastlogin:date: " + Bytes.toString(val8));
        
        byte[] val9 = rr.getValue(toBytes("Lastlogin"), toBytes("time"));
        System.out.println("Lastlogin:time " + Bytes.toString(val9));
        
        byte[] val10 = rr.getValue(toBytes("Lastlogin"), toBytes("success"));
        System.out.println("Lastlogin:success " + Bytes.toString(val10));
        System.out.println("-------------------------------------");
    }
    
}
