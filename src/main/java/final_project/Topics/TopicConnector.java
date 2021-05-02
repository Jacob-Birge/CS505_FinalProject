package final_project.Topics;

import final_project.Utils;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import final_project.Launcher;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopicConnector {
    private Gson gson;
    final Type typeOf = new TypeToken<List<Map<String,String>>>(){}.getType();
    private final Integer maxNumTuples = 1000;
    private static Connection connection;
    private static Channel channel;

    private String EXCHANGE_NAME = "patient_data";

    public TopicConnector() {
        gson = new Gson();
    }

    private static boolean oneAdded = false;
    public void connect() {
        try {
            String hostname = "vcbumg2.cs.uky.edu";//"128.163.202.50";
            String username = "student";
            String password = "student01";
            String virtualhost = "3";

            ConnectionFactory factory = new ConnectionFactory();
            factory.setRequestedHeartbeat(5);
            factory.setHost(hostname);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualhost);
            connection = factory.newConnection();
            channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, EXCHANGE_NAME, "#");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");

                List<Map<String,String>> incomingList = gson.fromJson(message, typeOf);
                if (!oneAdded){
                    Map<String, String> fakePer = new HashMap<>();
                    fakePer.put("first_name", "butt");
                    fakePer.put("last_name", "licker");
                    fakePer.put("mrn", "42069");
                    fakePer.put("zip_code", "40207");
                    fakePer.put("patient_status_code", "3");
                    incomingList.add(fakePer);
                    Map<String, String> fakePer2 = new HashMap<>();
                    fakePer2.put("first_name", "butter");
                    fakePer2.put("last_name", "man");
                    fakePer2.put("mrn", "42068");
                    fakePer2.put("zip_code", "40202");
                    fakePer2.put("patient_status_code", "6");
                    incomingList.add(fakePer2);
                    oneAdded = true;
                }
                //print and add message to cep input stream
                System.out.println(Utils.Color.BLUE+"[x] Received Batch"+Utils.Color.RESET + " '" +
                        delivery.getEnvelope().getRoutingKey() + "' (minus first_name and last_name)");
                for(Map<String,String> map : incomingList) {
                    System.out.println(Utils.Color.PURPLE+"INPUT CEP EVENT: "+Utils.Color.RESET + 
                    "{mrn=" +  map.get("mrn") + 
                    ", zip_code=" + map.get("zip_code") + 
                    ", patient_status_code=" + map.get("patient_status_code") + "}");
                    Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
                }
                //assign all incoming people to a hosital, home, or no assignment
                long startTime = System.currentTimeMillis();
                incomingList = Launcher.edbEngine.assignToHospital(incomingList);
                //incomingList = Launcher.edbEngine.newAssignToHospital(incomingList);
                long endTime = System.currentTimeMillis();
                System.out.println("assignToHospital time: " + (endTime - startTime) +" ms\t" + ((double)(endTime - startTime)/incomingList.size()) + " ms/patient");
                System.out.println();
                //add people to patient info table
                String queryBegin = "INSERT INTO PATIENTINFO (first_name, last_name, mrn, zipcode, patient_status_code, hospital_id) VALUES ";
                String insertTuples = "";
                Integer numTuples = 0;
                for(Map<String,String> map : incomingList) {
                    if (insertTuples != "")
                        insertTuples += ",";
                    insertTuples += "('"+map.get("first_name")+"','"+map.get("last_name")+"','"+map.get("mrn")+"','"+map.get("zip_code")+"','"+map.get("patient_status_code")+"',"+map.get("closest_hospital")+")";
                    numTuples++;
                    if (numTuples >= maxNumTuples){
                        String insertQuery = queryBegin + insertTuples;
                        Launcher.edbEngine.executeUpdate(insertQuery);
                        insertTuples = "";
                        numTuples = 0;
                    }
                    
                    // Update Hospital Bed Count 
                    String closestHosp = map.get("closest_hospital");
                    if (!closestHosp.equals("0") && !closestHosp.equals("-1")){
                        Integer hospitalAsInt = Integer.parseInt(closestHosp);
                        Launcher.edbEngine.executeUpdate("UPDATE HOSPITALS SET used_beds = used_beds + 1 WHERE id=" + hospitalAsInt);
                    }
                    
                }
                if (numTuples > 0){
                    String insertQuery = queryBegin + insertTuples;
                    Launcher.edbEngine.executeUpdate(insertQuery);
                    insertTuples = "";
                    numTuples = 0;
                }
                System.out.println("");

                
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
}

    public boolean disconnect(){
        try {
            channel.close();
            connection.close();
            return true;
        }
        catch (Exception ex){ return false; }
    }
}