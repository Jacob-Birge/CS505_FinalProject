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
    public static EDBEngine edbEngine = null;

    public static void main(String[] args) throws IOException {
        //Embedded database initialization
        System.out.println(Color.CYAN+"Starting EDB..."+Color.RESET);
        edbEngine = new EDBEngine();
        System.out.println(Color.GREEN+"EDB Started..."+Color.RESET);
        System.out.println();

        //Loading given csvs into embedded database
        System.out.println(Color.CYAN+"Loading kyzipdistance..."+Color.RESET);
        long startTime = System.currentTimeMillis();
        try{
            String insertQuery = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK (null,'KYZIPDISTANCE','../data/kyzipdistance.csv',',',null,null,0,1)";
            edbEngine.executeUpdate(insertQuery);
            System.out.println(Color.GREEN+"Loaded kyzipdistance..."+Color.RESET);
        }
        catch (Exception e){
            System.out.println(Color.RED+"Failed to load kyzipdistance"+Color.RESET);
            System.out.println(Color.YELLOW+"Make sure that data/kyzipdistance.csv exists"+Color.RESET);
            return;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Total load time: " + ((endTime - startTime)/1000.0) + " seconds");
        System.out.println();

        System.out.println(Color.CYAN+"Loading kyzipdetails..."+Color.RESET);
        startTime = System.currentTimeMillis();
        try{
            String insertQuery = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK (null,'KYZIPDETAILS','../data/kyzipdetails.csv',',',null,null,0,1)";
            edbEngine.executeUpdate(insertQuery);
            System.out.println(Color.GREEN+"Loaded kyzipdetails..."+Color.RESET);
        }
        catch (Exception e){
            System.out.println(Color.RED+"Failed to load kyzipdetails"+Color.RESET);
            System.out.println(Color.YELLOW+"Make sure that data/kyzipdetails.csv exists"+Color.RESET);
            return;
        }
        endTime = System.currentTimeMillis();
        System.out.println("Total load time: " + ((endTime - startTime)/1000.0) + " seconds");
        System.out.println();

        System.out.println(Color.CYAN+"Loading hospitals..."+Color.RESET);
        startTime = System.currentTimeMillis();
        try{
            String insertQuery = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE_BULK (null,'HOSPITALS','../data/hospitals.csv',',',null,null,0,1)";
            edbEngine.executeUpdate(insertQuery);
            System.out.println(Color.GREEN+"Loaded hospitals..."+Color.RESET);
        }
        catch (Exception e){
            System.out.println(Color.RED+"Failed to load hospitals"+Color.RESET);
            System.out.println(Color.YELLOW+"Make sure that data/hospitals.csv exists"+Color.RESET);
            return;
        }
        endTime = System.currentTimeMillis();
        System.out.println("Total load time: " + ((endTime - startTime)/1000.0) + " seconds");
        System.out.println();

        System.out.println(Color.CYAN+"Starting CEP..."+Color.RESET);
        //Embedded database initialization
        cepEngine = new CEPEngine();

        //START MODIFY
        inputStreamName = "PatientInStream";
        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";

        String outputStreamName = "PatientOutStream";
        String outputStreamAttributesString = "patient_status_code string, count long";

        String queryString = " " +
                "from PatientInStream#window.timeBatch(5 sec) " +
                "select patient_status_code, count() as count " +
                "group by patient_status_code " +
                "insert into PatientOutStream; ";

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
