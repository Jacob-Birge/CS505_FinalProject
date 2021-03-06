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
    public boolean connect() {
        try {
            String hostname = "vcbumg2.cs.uky.edu";//"128.163.202.50";
            String username = "student";
            String password = "student01";
            String virtualhost = "3";

            ConnectionFactory factory = new ConnectionFactory();
            factory.setRequestedHeartbeat(30);
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
                if (Launcher.addFakePeople && !oneAdded){
                    Map<String, String> fakePer = new HashMap<>();
                    fakePer.put("first_name", "john");
                    fakePer.put("last_name", "smith");
                    fakePer.put("mrn", "42069");
                    fakePer.put("zip_code", "40207");
                    fakePer.put("patient_status_code", "3");
                    incomingList.add(fakePer);
                    Map<String, String> fakePer2 = new HashMap<>();
                    fakePer2.put("first_name", "jane");
                    fakePer2.put("last_name", "doe");
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
                incomingList = Launcher.edbEngine.assignToHospital(incomingList);
                //add people to patient info table
                String insertQueryBegin = "INSERT INTO PATIENTINFO (first_name, last_name, mrn, zipcode, patient_status_code, hospital_id) VALUES ";
                String insertTuples = "";
                Integer numInsertTuples = 0;
                //loop across incoming patients
                for(Map<String,String> map : incomingList) {
                    if (insertTuples != "")
                        insertTuples += ",";
                    insertTuples += "('"+map.get("first_name")+"','"+map.get("last_name")+"','"+map.get("mrn")+"','"+map.get("zip_code")+"','"+map.get("patient_status_code")+"',"+map.get("closest_hospital")+")";
                    numInsertTuples++;
                    //if exceeding max number for insert, go ahead and insert
                    if (numInsertTuples >= maxNumTuples){
                        String insertQuery = insertQueryBegin + insertTuples;
                        Launcher.edbEngine.executeUpdate(insertQuery);
                        insertTuples = "";
                        numInsertTuples = 0;
                    }
                }
                if (numInsertTuples > 0){
                    String insertQuery = insertQueryBegin + insertTuples;
                    Launcher.edbEngine.executeUpdate(insertQuery);
                    insertTuples = "";
                    numInsertTuples = 0;
                }
                System.out.println("");
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
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