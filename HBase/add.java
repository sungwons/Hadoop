
package useradmin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class add {
    
    public static void main(String[] args) throws IOException{
    
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "User");
        
        String add = args[0];
        String rowid = args[1];
        String email= args[2];
        String password = args[3];
        String status = args[4];
        String date_of_birth= args[5];
        String security_question = args[6];
        String security_answer = args[7];        
                
        Put put1 = new Put(toBytes(rowid));
        
        put1.add(toBytes("creds"), toBytes("email"), toBytes(email));
        put1.add(toBytes("creds"), toBytes("password"), toBytes(password));
        put1.add(toBytes("prefs"), toBytes("status"), toBytes(status));
        put1.add(toBytes("prefs"), toBytes("date_of_birth"), toBytes(date_of_birth));
        put1.add(toBytes("prefs"), toBytes("security_question"), toBytes(security_question));
        put1.add(toBytes("prefs"), toBytes("security_answer"), toBytes(security_answer));
        
        hTable.put(put1);
        
        System.out.println("Successed");
        
        hTable.close();

    }
    
}
