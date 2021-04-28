package final_project;

import final_project.CEP.CEPEngine;
import final_project.EDB.EDBEngine;
import final_project.Topics.TopicConnector;
import final_project.Utils.Color;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentSkipListSet;


public class Launcher {

    public static final String API_SERVICE_KEY = "12142047"; //Change this to your student id
    public static final int WEB_PORT = 9000;
    public static String inputStreamName = null;

    public static Integer app_status_code = 0;
    public static Boolean ableToReset = false;
    public static ConcurrentSkipListSet<String> alertZipcodes = new ConcurrentSkipListSet<String>();
    public static Long posCount = 0l;
    public static Long negCount = 0l;

    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;

    public static EDBEngine edbEngine = null;

    public static void main(String[] args) throws IOException {
        app_status_code = 0;
        ableToReset = false;
        //Embedded HTTP initialization
        startServer();
        System.out.println();

        //Embedded database initialization
        System.out.println(Color.CYAN+"Starting EDB..."+Color.RESET);
        edbEngine = new EDBEngine();
        System.out.println(Color.GREEN+"EDB Started..."+Color.RESET);
        System.out.println();

        //Loading given csvs into embedded database
        long startTime = System.currentTimeMillis();
        loadCSV("kyzipdistance");
        long endTime = System.currentTimeMillis();
        System.out.println("Load time: " + ((endTime - startTime)/1000.0) + " seconds");
        System.out.println();

        startTime = System.currentTimeMillis();
        loadCSV("kyzipdetails");
        endTime = System.currentTimeMillis();
        System.out.println("Load time: " + ((endTime - startTime)/1000.0) + " seconds");
        System.out.println();

        startTime = System.currentTimeMillis();
        loadCSV("hospitals");
        endTime = System.currentTimeMillis();
        System.out.println("Load time: " + ((endTime - startTime)/1000.0) + " seconds");
        System.out.println();

        System.out.println(Color.CYAN+"Starting CEP..."+Color.RESET);
        inputStreamName = "PatientInStream";
        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";

        String[] outputStreamNames = {"RTR1OutStream", "RTR2OutStream", "RTR3OutStream"};
        String[] outputStreamAttributesStrings = {"e1count long, e2count long", "count long", "count long, isNeg bool"};

        String rtr1Query = " " +
                " from PatientInStream[patient_status_code == '2' or patient_status_code == '5' or patient_status_code == '6']#window.time(15 sec)" +
                " select count() as count" +
                " insert into tempStream;" +

                " from every( e1=tempStream )" +
                " -> e2=tempStream[(2*e1.count) <= count]" +
                " within 15 sec" +
                " select e1.count as e1count, e2.count as e2count" +
                " insert into RTR1OutStream;";

        String rtr2Query = " ";/* +
                " from PatientInStream" +
                " select count() as count" +
                " insert into RTR2OutStream;";*/

        String rtr3Query = " " +
                " from PatientInStream[patient_status_code == '1' or patient_status_code == '4']" +
                " select count(mrn) as count, true as isNeg" +
                " insert into RTR3OutStream;" +
                " from PatientInStream[patient_status_code == '2' or patient_status_code == '5' or patient_status_code == '6']" +
                " select count(mrn) as count, false as isNeg" +
                " insert into RTR3OutStream;";

        String queryString = rtr1Query + " " + rtr2Query + " " + rtr3Query;

        //CEP initialization
        cepEngine = new CEPEngine(inputStreamName, outputStreamNames, inputStreamAttributesString, outputStreamAttributesStrings, queryString);
        System.out.println(Color.GREEN+"CEP Started..."+Color.RESET);
        System.out.println();

        //starting Collector
        System.out.println(Color.CYAN+"Starting TopicConnector..."+Color.RESET);
        topicConnector = new TopicConnector();
        topicConnector.connect();
        System.out.println(Color.GREEN+"TopicConnector Started..."+Color.RESET);
        System.out.println();

        app_status_code = 1;
        ableToReset = true;

        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void loadCSV(String csvName){
        System.out.println(Color.CYAN+"Loading "+csvName+"..."+Color.RESET);
        try{
            String insertQuery = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK (null,'"+csvName.toUpperCase()+"','../data/"+csvName+".csv',',',null,null,0,1)";
            edbEngine.executeUpdate(insertQuery);
            System.out.println(Color.GREEN+"Loaded "+csvName+"..."+Color.RESET);
        }
        catch (Exception e){
            System.out.println(Color.RED+"Failed to load "+csvName+""+Color.RESET);
            System.out.println(Color.YELLOW+"Make sure that data/"+csvName+".csv exists"+Color.RESET);
            return;
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("final_project.httpcontrollers");

        System.out.println(Color.CYAN + "Starting Web Server..." + Color.RESET);
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println(Color.GREEN + "Web Server Started..." + Color.RESET);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO: causes issues when rabbitmq is unresponsive
    public static boolean reset(){
        try {
            ableToReset = false;
            app_status_code = 0;
            System.out.println(Color.CYAN+"Pausing TopicConnector..."+Color.RESET);
            if (!topicConnector.disconnect()) return false;

            System.out.println(Color.CYAN+"Resetting CEP..."+Color.RESET);
            cepEngine.reset();

            System.out.println(Color.CYAN+"Purging Patient Info..."+Color.RESET);
            if (!edbEngine.purgePatientInfo()) return false;
            
            alertZipcodes.clear();
            posCount = 0l;
            negCount = 0l;

            System.out.println(Color.CYAN+"Reconnecting TopicConnector..."+Color.RESET);
            topicConnector.connect();

            System.gc();

            ableToReset = true;
            app_status_code = 1;
            return true;
        }
        catch (Exception ex){}
        return false;
    }
}
