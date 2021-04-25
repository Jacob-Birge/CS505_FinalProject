package final_project.CEP;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import final_project.Launcher;
import final_project.Utils.Color;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {
    private String topic;
    private Gson gson;
    final Type typeOf = new TypeToken<List<Map<String,Map<String,Object>>>>(){}.getType();

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
        gson = new Gson();
    }

    @Override
    public void onMessage(Object msg) {
        try {
            System.out.println(Color.CYAN+"OUTPUT EVENT: "+Color.RESET + msg);
            List<Map<String,Map<String,Object>>> msgList = gson.fromJson((String)msg, typeOf);
            for(Map<String,Map<String,Object>> map : msgList) {
                System.out.println(Color.PURPLE+"OUTPUT CEP EVENT: "+Color.RESET +  map.get("event").get("count"));
            }
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
