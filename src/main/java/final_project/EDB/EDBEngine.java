package final_project.EDB;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EDBEngine {

    private DataSource ds;

    public EDBEngine() {

        try {
            //Name of database
            String databaseName = "myDatabase";

            //Driver needs to be identified in order to load the namespace in the JVM
            String dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(dbDriver).newInstance();

            //Connection string pointing to a local file location
            String dbConnectionString = "jdbc:derby:memory:" + databaseName + ";create=true";
            ds = setupDataSource(dbConnectionString);

            /*
            if(!databaseExist(databaseName)) {
                System.out.println("No database, creating " + databaseName);
                initDB();
            } else {
                System.out.println("Database found, removing " + databaseName);
                delete(Paths.get(databaseName).toFile());
                System.out.println("Creating " + databaseName);
                initDB();
            }
             */

            initDB();
        }

        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        connectionFactory = new DriverManagerConnectionFactory(connectURI, null);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public void initDB() {
        String createRNode = "CREATE TABLE APP.KYZIPDISTANCE" +
                "(" +
                "   zip_from INTEGER," +
                "   zip_to INTEGER," +
                "   distance DOUBLE" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        createRNode = "CREATE TABLE APP.KYZIPDETAILS" +
                "(" +
                "   zip INTEGER," +
                "   name VARCHAR(255)," +
                "   city VARCHAR(255)," +
                "   state VARCHAR(255)," +
                "   county VARCHAR(255)" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        createRNode = "CREATE TABLE APP.HOSPITALS" +
                "(" +
                "   id BIGINT," +
                "   zip INTEGER," +
                "   beds INTEGER," +
                "   trauma VARCHAR(255)," +
                "   used_beds INTEGER" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        createRNode = "CREATE TABLE PATIENTINFO" +
                "(" +
                "   first_name VARCHAR(255)," +
                "   last_name VARCHAR(255)," +
                "   mrn VARCHAR(255)," +
                "   zipcode VARCHAR(255)," +
                "   patient_status_code VARCHAR(255)," +
                "   hospital_id BIGINT" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DROP TABLE " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /*
    public boolean databaseExist(String databaseName)  {
        return Paths.get(databaseName).toFile().exists();
    }
    */
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        try {

            if(!ds.getConnection().isClosed()) {
                exist = true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

        ResultSet result;
        DatabaseMetaData metadata = null;

        try {
            metadata = ds.getConnection().getMetaData();
            result = metadata.getTables(null, null, tableName.toUpperCase(), null);

            if(result.next()) {
                exist = true;
            }
        } catch(java.sql.SQLException e) {
            e.printStackTrace();
        }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }


    //gets the hospital id or "Home" from patient's mrn number
    public String getPatientLocation(String mrn) {
        try {
            String queryString = null;
            queryString = "SELECT hospital_id AS hid FROM APP.PATIENTINFO WHERE mrn = '" + mrn + "'";
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    try(ResultSet rs = stmt.executeQuery(queryString)) {
                        while (rs.next()) {
                            return rs.getString("hid");
                        }
                    }
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    // Gets hospital info
    public Map<String,String> getHospitalInfo(String ID) {
        Map<String,String> responseMap = null;
        try {
            responseMap = new HashMap<>();
            String queryString = null;
            //fill in the query
            queryString = "SELECT beds, beds - used_beds AS availbeds, zip FROM APP.HOSPITALS WHERE id = " + ID;
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    try(ResultSet rs = stmt.executeQuery(queryString)) {
                        while (rs.next()) {
                            responseMap.put("total_beds", rs.getString("beds"));
                            responseMap.put("avalable_beds", rs.getString("availbeds"));
                            responseMap.put("zipcode", rs.getString("zip"));
                        }
                    }
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return responseMap;
    }

    // assigns a patient to a hospital if neccessary.
    public List<Map<String,String>> assignToHospital(List<Map<String,String>> incoming){
        try {
            for(Map<String,String> map : incoming) {
                // if patient needs a hospital
                if (map.get("patient_status_code").equals("3") || map.get("patient_status_code").equals("5") || map.get("patient_status_code").equals("6")) {
                    String pzip = map.get("zip_code");
                    String queryString = "";
                    if (map.get("patient_status_code").equals("6")) {
                        queryString =   "SELECT h.id AS hid FROM APP.HOSPITALS AS h WHERE 'h.zip' = '" + 
                                        pzip + "' AND h.trauma != 'NOT AVAILABLE' AND h.beds > h.used_beds";
                    }
                    else {
                        queryString =   "SELECT h.id AS hid FROM APP.HOSPITALS AS h WHERE 'h.zip' = '" + 
                                        pzip + "' AND h.beds > h.used_beds";
                    }
                    try(Connection conn = ds.getConnection()) {
                        try (Statement stmt = conn.createStatement()) {
                            try(ResultSet rs = stmt.executeQuery(queryString)) {
                                if(rs.next()) {
                                    // in the same zip code
                                    map.put("closest_hospital", rs.getString("hid"));
                                }
                                else {
                                    // get closest hospital
                                    String hidForClosestHospital = "";
                                    if (map.get("patient_status_code").equals("6")) {
                                        hidForClosestHospital = findClosestHospital(pzip, true);
                                    }
                                    else {
                                        hidForClosestHospital = findClosestHospital(pzip, false);
                                    }
                                    map.put("closest_hospital", hidForClosestHospital);
                                }
                            }
                        }
                    }
                }
                else {
                    //setting home to 0 bc hospital code is big int, converted to "Home" in API.java
                    map.put("closest_hospital", "0"); 
                }
            }
            return incoming;
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    // finds a patient's closest hospital
    public String findClosestHospital(String zip, Boolean isStatus6) {
        Integer zipc = Integer.parseInt(zip);
        try {
            String queryString = ""; 
            if (isStatus6) {
                queryString =   "SELECT h.id AS hid FROM APP.KYZIPDISTANCE as z, APP.HOSPITALS as h WHERE z.zip_from = " + 
                                zipc + " AND z.zip_to = h.zip AND h.trauma != 'NOT AVAILABLE' AND h.beds > h.used_beds ORDER BY z.distance ASC";
            }
            else {
                queryString =   "SELECT h.id AS hid FROM APP.KYZIPDISTANCE as z, APP.HOSPITALS as h WHERE z.zip_from = " + 
                                zipc + " AND z.zip_to = h.zip AND h.beds > h.used_beds ORDER BY z.distance ASC";
            }
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    try(ResultSet rs = stmt.executeQuery(queryString)) {
                        if(rs.next()) {
                            return rs.getString("hid");
                        }
                        else {
                            System.out.println("Zip Not Found: "+zipc+"\n");
                            String alternateHosp = "";
                            if (isStatus6) {
                                alternateHosp = "SELECT h.id AS hid FROM APP.HOSPITALS AS h WHERE h.trauma != 'NOT AVAILABLE' " +
                                                "AND h.beds > h.used_beds ORDER BY used_beds ASC";
                            }
                            else {
                                alternateHosp = "SELECT h.id AS hid FROM APP.HOSPITALS AS h WHERE h.beds > h.used_beds " +
                                                "ORDER BY used_beds ASC";
                            }
                            
                            try(Connection con = ds.getConnection()) {
                                try (Statement stamt = con.createStatement()) {
                                    try(ResultSet rst = stamt.executeQuery(alternateHosp)) {
                                        if(rst.next()) {
                                            return rst.getString("hid");
                                        }
                                        else {
                                            return "-1";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
