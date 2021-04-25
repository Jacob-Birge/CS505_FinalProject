package final_project;

import final_project.CEP.CEPEngine;
import final_project.EDB.EDBEngine;
import final_project.Topics.TopicConnector;
import final_project.Utils.Color;
import final_project.httpfilters.AuthenticationFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


public class Launcher {

    public static final String API_SERVICE_KEY = "12142047"; //Change this to your student id
    public static final int WEB_PORT = 9000;
    public static String inputStreamName = null;
    public static double accessCount = -1;

    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;
    public static CEPEngine cepRTR3 = null;
    public static EDBEngine edbEngine = null;

    public static void main(String[] args) throws IOException {
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
        //Embedded database initialization
        cepEngine = new CEPEngine();

        //START MODIFY
        inputStreamName = "PatientInStream";
        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";

        String outputStreamName = "PatientOutStream";
        String outputStreamAttributesString = "s1ZipCode string, count long";

        String queryString = " " +
                "from PatientInStream[patient_status_code == '2' or patient_status_code == '5' or patient_status_code == '6']#window.timeBatch(15 sec)" +
                " select zip_code as s1ZipCode, count() as count" +
                " group by zip_code" +
                " insert into PatientOutStream;";

        //END MODIFY

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println(Color.GREEN+"CEP Started..."+Color.RESET);
        System.out.println();
        
        //Embedded HTTP initialization
        startServer();
        System.out.println();

        //starting Collector
        System.out.println(Color.CYAN+"Starting TopicConnector..."+Color.RESET);
        topicConnector = new TopicConnector();
        topicConnector.connect();
        System.out.println(Color.GREEN+"TopicConnector Started..."+Color.RESET);
        System.out.println();

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
        .packages("final_project.httpcontrollers")
        .register(AuthenticationFilter.class);

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

}
