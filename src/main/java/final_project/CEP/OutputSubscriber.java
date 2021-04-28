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
    private String streamName;
    private Gson gson;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
        this.streamName = streamName;
        gson = new Gson();
    }

    @Override
    public void onMessage(Object msg) {
        try {
            if (streamName == "RTR1OutStream"){
                Type typeOf = new TypeToken<Map<String,Map<String,Object>>>(){}.getType();
                Map<String,Map<String,Object>> msgList = gson.fromJson((String)msg, typeOf);
                String zipCode = (String)msgList.get("event").get("zip_code");
                long alertTime = System.currentTimeMillis();
                Launcher.alertZipcodes.put(zipCode, alertTime);
                
                System.out.println(Color.CYAN+"OUTPUT EVENT: "+Color.RESET + (String)msg + " " + streamName);
                System.out.println("");
            }
            else if (streamName == "RTR3OutStream"){
                Type typeOf = new TypeToken<Map<String,Map<String,Object>>>(){}.getType();
                Map<String,Map<String,Object>> msgList = gson.fromJson((String)msg, typeOf);
                Long count = (long)((double)msgList.get("event").get("count"));
                if ((Boolean)msgList.get("event").get("isNeg")){
                    Launcher.negCount = count;
                }
                else{
                    Launcher.posCount = count;
                }
            }
            else{
                System.out.println(Color.RED+"Unknown Stream Encountered"+Color.RESET);
                System.out.println("");
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public String getTopic() {
        return topic;
    }
}
