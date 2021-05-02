package final_project.CEP;

import com.google.gson.Gson;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CEPEngine {

    private SiddhiManager siddhiManager;
    private SiddhiAppRuntime siddhiAppRuntime;
    private Map<String,String> topicMap;
    private String inputStreamName;
    private String[] outputStreamName;
    private String inputStreamAttributesString;
    private String[] outputStreamAttributesString;
    private String queryString;

    public CEPEngine(String inputStreamName, String[] outputStreamName, String inputStreamAttributesString, String[] outputStreamAttributesString,String queryString) {
        Class JsonClassSource = null;
        Class JsonClassSink = null;

        try {
            JsonClassSource = Class.forName("io.siddhi.extension.map.json.sourcemapper.JsonSourceMapper");
            JsonClassSink = Class.forName("io.siddhi.extension.map.json.sinkmapper.JsonSinkMapper");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try {
            InMemorySink sink = new InMemorySink();
            sink.connect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        topicMap = new ConcurrentHashMap<>();

        // Creating Siddhi Manager
        siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sourceMapper:json",JsonClassSource);
        siddhiManager.setExtension("sinkMapper:json",JsonClassSink);

        this.inputStreamName = inputStreamName;
        this.outputStreamName = outputStreamName;
        this.inputStreamAttributesString = inputStreamAttributesString;
        this.outputStreamAttributesString = outputStreamAttributesString;
        this.queryString = queryString;

        createCEP();
    }

    private boolean createCEP() {
        try {
            String inputTopic = UUID.randomUUID().toString();

            topicMap.put(inputStreamName,inputTopic);
            for(String name : outputStreamName) {
                String outputTopic = UUID.randomUUID().toString();
                topicMap.put(name,outputTopic);
            }

            String sourceString = getSourceString(inputStreamAttributesString, inputTopic, inputStreamName);
            String sinkString = "";
            for(int i=0; i<outputStreamName.length; i++) {
                String name = outputStreamName[i];
                String attr = outputStreamAttributesString[i];
                sinkString += getSinkString(topicMap.get(name),name,attr) + " ";
            }
            //Generating runtime

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(sourceString + " " + sinkString + " " + queryString);

            for(String name : outputStreamName) {
                InMemoryBroker.Subscriber subscriberTest = new OutputSubscriber(topicMap.get(name),name);
                //subscribe to "inMemory" broker per topic
                InMemoryBroker.subscribe(subscriberTest);
            }

            //Starting event processing
            siddhiAppRuntime.start();
            return true;
            } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public void input(String streamName, String jsonPayload) {
        try {
            if (topicMap.containsKey(streamName)) {
                InMemoryBroker.publish(topicMap.get(streamName), jsonPayload);
            } else {
                System.out.println("input error : no schema");
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    private String getSourceString(String inputStreamAttributesString, String topic, String streamName) {
        String sourceString = null;
        try {
            sourceString  = "@source(type='inMemory', topic='" + topic + "', @map(type='json')) " +
                    "define stream " + streamName + " (" + inputStreamAttributesString + "); ";
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sourceString;
    }

    private String getSinkString(String topic, String streamName, String outputSchemaString) {
        String sinkString = null;
        try {
            sinkString = "@sink(type='inMemory', topic='" + topic + "', @map(type='json')) " +
                    "define stream " + streamName + " (" + outputSchemaString + "); ";
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return sinkString;
    }

    public boolean reset(){
        try {
            siddhiAppRuntime.shutdown();
            siddhiManager.shutdown();

            if (!createCEP()) return false;
            return true;
        }
        catch (Exception ex) {}
        return false;
    }
}
