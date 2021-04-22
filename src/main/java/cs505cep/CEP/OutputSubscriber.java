package cs505cep.CEP;

import cs505cep.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT EVENT: " + msg);
            String[] sstr = String.valueOf(msg).split(":");
            String[] outval = sstr[2].split("}");
            Launcher.accessCount = Double.parseDouble(outval[0]);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
