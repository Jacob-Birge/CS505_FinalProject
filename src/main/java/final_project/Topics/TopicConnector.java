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
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;


public class TopicConnector {
    private Gson gson;
    final Type typeOf = new TypeToken<List<Map<String,String>>>(){}.getType();
    private final Integer maxNumTuples = 1000;

    private String EXCHANGE_NAME = "patient_data";

    public TopicConnector() {
        gson = new Gson();
    }

    public void connect() {
        try {
            String hostname = "128.163.202.50";
            String username = "student";
            String password = "student01";
            String virtualhost = "3";

            ConnectionFactory factory = new ConnectionFactory();
            factory.setRequestedHeartbeat(30);
            factory.setHost(hostname);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualhost);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, EXCHANGE_NAME, "#");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");

                List<Map<String,String>> incomingList = gson.fromJson(message, typeOf);
                System.out.println(Utils.Color.BLUE+"[x] Received Batch"+Utils.Color.RESET + " '" +
                        delivery.getEnvelope().getRoutingKey() + "':"+incomingList.size());
                for(Map<String,String> map : incomingList) {
                    System.out.println(Utils.Color.PURPLE+"INPUT CEP EVENT: "+Utils.Color.RESET +  map);
                    Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
                }
                Launcher.edbEngine.executeUpdate("UPDATE HOSPITALS SET used_beds = used_beds + "+incomingList.size()+" WHERE id=11640536");
                /*
                String tempQuery = "SELECT id FROM HOSPITALS";
                ResultSet rs = Launcher.edbEngine.executeSelect(tempQuery);
                try {
                    while (rs.next()) {
                        System.out.println(rs.getInt("id") + " ");
                    }
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
                */
                String queryBegin = "INSERT INTO PATIENTINFO (first_name, last_name, mrn, zipcode, patient_status_code, hospital_id) VALUES ";
                String insertTuples = "";
                Integer numTuples = 0;
                for(Map<String,String> map : incomingList) {
                    if (insertTuples != "")
                        insertTuples += ",";
                    insertTuples += "('"+map.get("first_name")+"','"+map.get("last_name")+"','"+map.get("mrn")+"','"+map.get("zip_code")+"','"+map.get("patient_status_code")+"', 11640536)";
                    numTuples++;
                    if (numTuples >= maxNumTuples){
                        String insertQuery = queryBegin + insertTuples;
                        Launcher.edbEngine.executeUpdate(insertQuery);
                        insertTuples = "";
                        numTuples = 0;
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

}