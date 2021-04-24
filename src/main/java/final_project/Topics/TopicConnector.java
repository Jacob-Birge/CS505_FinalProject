package final_project.Topics;

import final_project.Utils;
import final_project.Utils.Color;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import final_project.Launcher;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;


public class TopicConnector {

    private Gson gson;
    final Type typeOf = new TypeToken<List<Map<String,String>>>(){}.getType();

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
            factory.setRequestedHeartbeat(60);
            factory.setHost(hostname);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualhost);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, EXCHANGE_NAME, "#");


            System.out.println(Color.BLUE+"[*] Waiting for messages. To exit press CTRL+C"+Color.RESET);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(Color.BLUE+"[x] Received Batch '"+Color.RESET +
                        delivery.getEnvelope().getRoutingKey() + "':");

                List<Map<String,String>> incomingList = gson.fromJson(message, typeOf);
                for(Map<String,String> map : incomingList) {
                    System.out.println(Color.PURPLE+"INPUT CEP EVENT: "+Color.RESET +  map);
                    Launcher.cepEngine.input(Launcher.inputStreamName, gson.toJson(map));
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