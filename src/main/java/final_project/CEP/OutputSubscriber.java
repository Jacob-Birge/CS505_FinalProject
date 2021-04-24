package final_project.CEP;

import final_project.Launcher;
import final_project.Utils.Color;
import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println(Color.CYAN+"OUTPUT EVENT: "+Color.RESET + msg);
            System.out.println("");
            //String[] sstr = String.valueOf(msg).split(":");
            //String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Double.parseDouble(outval[0]);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
