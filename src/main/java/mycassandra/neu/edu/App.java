package mycassandra.neu.edu;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;


import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) 
    		throws TException, InvalidRequestException, UnavailableException, UnsupportedEncodingException, NotFoundException,IOException
    {
        System.out.println( "Hello World!" );
        
        TTransport tr = new TFramedTransport(new TSocket("127.0.0.1", 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        final Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        String key_user_id = "1";

        // insert data
        long timestamp = System.currentTimeMillis();
        

       client.set_keyspace("thriftKeyspace1");     
       
     //  CfDef cf = new CfDef();
     //  cf.setName("books");
     //  cf.setKeyspace("thriftKeyspace");
     //  client.system_add_column_family(cf);
       
      
       loadUsers(client);
      // loadBooks(client);
      // loadRatings(client);
        


        tr.close();
    }
    
    public static ByteBuffer toByteBuffer(String value) 
    throws UnsupportedEncodingException
    {
        return ByteBuffer.wrap(value.getBytes("UTF-8"));
    }
        
    public static String toString(ByteBuffer buffer) 
    throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }
    
    
    public static void loadUsers(Cassandra.Client client) throws TException, InvalidRequestException, UnavailableException, UnsupportedEncodingException, NotFoundException,IOException{
    int totalUsers=0;
    try {
    	
    	
    	System.out.println("Working on Users data!");
    	
    	client.truncate("Users");
    	ColumnParent parent = new ColumnParent("Users");
 
    
        Column col_userid = new Column(toByteBuffer("userID"));
    	Column col_location = new Column(toByteBuffer("location"));
    	Column col_age = new Column(toByteBuffer("age"));
    	
        BufferedReader br = new BufferedReader(new FileReader("src/main/resources/BX-Users.csv"));
    	String readFile = br.readLine();
        while (readFile != null) {
           
        	String[] tokenDelimiter = readFile.split(";");
            String userID = tokenDelimiter[0];
            String location = tokenDelimiter[1];
            String age = tokenDelimiter[2];
           

            readFile = br.readLine();
            totalUsers = totalUsers+1;
        
         
          
        	//System.out.println("Document number :"+totalUsers+" ; userid: "+userID+" ; location"+ location + " ; age:"+ age);
        	
        	

        
           // col_userid.setValue(toByteBuffer(userID));
            col_age.setValue(toByteBuffer(age));
            col_age.setTimestamp(System.currentTimeMillis());
            client.insert(toByteBuffer(userID), parent, col_age, ConsistencyLevel.ONE);
            
            col_location.setValue(toByteBuffer(location));
            col_location.setTimestamp(System.currentTimeMillis());
            client.insert(toByteBuffer(userID), parent, col_location, ConsistencyLevel.ONE);
            
    

            /*
            ColumnPath path = new ColumnPath("Standard1");

            // read single column
            path.setColumn(toByteBuffer("name"));
            System.out.println(client.get(toByteBuffer(key_user_id), path, ConsistencyLevel.ONE));

            // read entire row
            SlicePredicate predicate = new SlicePredicate();
            SliceRange sliceRange = new SliceRange(toByteBuffer(""), toByteBuffer(""), false, 10);
            predicate.setSlice_range(sliceRange);
            
            List<ColumnOrSuperColumn> results = client.get_slice(toByteBuffer(key_user_id), parent, predicate, ConsistencyLevel.ONE);
            for (ColumnOrSuperColumn result : results)
            {
                Column column = result.column;
                System.out.println(toString(column.name) + " -> " + toString(column.value));
            }
            */

            
        }
        System.out.println("Total Ratings : "+ totalUsers);
        br.close();
    } catch (FileNotFoundException e) {
        System.out.println("File was not Found!");
    }
    }
    
    public static void loadBooks(Cassandra.Client client) throws TException, InvalidRequestException, UnavailableException, UnsupportedEncodingException, NotFoundException,IOException{
        int total=0;
        try {
        	
        	
        	System.out.println("Working on Books data!");
        	
        	client.truncate("Books");
        	ColumnParent parent = new ColumnParent("Books");
     
        
        	Column col_title = new Column(toByteBuffer("book-title"));
        	Column col_author = new Column(toByteBuffer("author"));
        	Column col_yearOfPublication = new Column(toByteBuffer("year-of-publication"));
        	Column col_publisher = new Column(toByteBuffer("publisher"));
        	Column col_urlS = new Column(toByteBuffer("image-url-s"));
        	Column col_urlM = new Column(toByteBuffer("image-url-m"));
        	Column col_urlL = new Column(toByteBuffer("image-url-l"));
        	
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/BX-Books.csv"));
        	String readFile = br.readLine();
            while (readFile != null) {
               
            	String[] tokenDelimiter = readFile.split(";");
                String isbn = tokenDelimiter[0];
                String title = tokenDelimiter[1];
                String author = tokenDelimiter[2];
                String yopublication = tokenDelimiter[3];
                String publisher = tokenDelimiter[4];
                String urls = tokenDelimiter[5];
                String urlm = tokenDelimiter[6];
                String urll = tokenDelimiter[7];

                readFile = br.readLine();
                total = total+1;
            
             
              
            	//System.out.println("Document number :"+total+" ; isbn: "+isbn+" ; title"+ title + " ; yopu:"+ yopublication
            	//		+" ;publisher:" +publisher+" ;urls: "+urls+ " ;urlm:"+urlm+" ;urll:"+urll);
            	
            	

            
                //set author
                col_author.setValue(toByteBuffer(author));
                col_author.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_author, ConsistencyLevel.ONE);
                
                //set title
                col_title.setValue(toByteBuffer(title));
                col_title.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_title, ConsistencyLevel.ONE);
               
                //set year of publication
                col_yearOfPublication.setValue(toByteBuffer(yopublication));
                col_yearOfPublication.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_yearOfPublication, ConsistencyLevel.ONE);
                
                //set publisher
                col_publisher.setValue(toByteBuffer(publisher));
                col_publisher.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_publisher, ConsistencyLevel.ONE);
                
                //set urls
                col_urlS.setValue(toByteBuffer(urls));
                col_urlS.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_urlS, ConsistencyLevel.ONE);
                
                //set urlm
                col_urlM.setValue(toByteBuffer(urlm));
                col_urlM.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_urlM, ConsistencyLevel.ONE);
                
                //set year of publication
                col_urlL.setValue(toByteBuffer(urll));
                col_urlL.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(isbn), parent, col_urlL, ConsistencyLevel.ONE);
                
          
            }
            System.out.println("Total Books : "+ total);
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("File was not Found!");
        }
        }
        
    public static void loadRatings(Cassandra.Client client) throws TException, InvalidRequestException, UnavailableException, UnsupportedEncodingException, NotFoundException,IOException{
        int total=0;
        try {
        	
        	
        	System.out.println("Working on Ratings data!");
        	
        	client.truncate("Ratings");
        	ColumnParent parent = new ColumnParent("Ratings");
     
        
        	Column col_isbn = new Column(toByteBuffer("isbn"));
        	Column col_bookRating = new Column(toByteBuffer("book-rating"));
        	
        	
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/BX-Book-Ratings.csv"));
        	String readFile = br.readLine();
            while (readFile != null) {
               
            	String[] tokenDelimiter = readFile.split(";");
                String userid = tokenDelimiter[0];
                String isbn = tokenDelimiter[1];
                String rating = tokenDelimiter[2];
              

                readFile = br.readLine();
                total = total+1;
            
             
              
            	//System.out.println("Document number :"+total+" ; isbn: "+isbn+"; rating " + rating+ ";userid :"+userid);
            	

            
                //set isbn
                col_isbn.setValue(toByteBuffer(isbn));
                col_isbn.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(userid), parent, col_isbn, ConsistencyLevel.ONE);
                
                //set rating
                col_bookRating.setValue(toByteBuffer(rating));
                col_bookRating.setTimestamp(System.currentTimeMillis());
                client.insert(toByteBuffer(userid), parent, col_bookRating, ConsistencyLevel.ONE);
               
            
                
          
            }
            System.out.println("Total Books ratings : "+ total);
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("File was not Found!");
        }
        }
        
}
