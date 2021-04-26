package final_project.httpcontrollers;

import com.google.gson.Gson;
import final_project.CEP.accessRecord;
import final_project.Launcher;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import final_project.Launcher;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    private void logToConsole(String apiCalled){
        //get remote ip address from request
        String remoteIP = request.get().getRemoteAddr();
        //get the timestamp of the request
        long access_ts = System.currentTimeMillis();
        System.out.println("IP: " + remoteIP + "\tTimestamp: " + access_ts + "\tAPI: " + apiCalled);
    }

    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response MF1(String authKey) {
        String responseString = "{}";
        try {
            logToConsole("getteam");
            
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "CEB: complex event brocessors");
            responseMap.put("team_member_sids", "[\"12257232\", \"12142047\"]");
            //TODO: logic for if app is on or off
            responseMap.put("app_status_code","0");

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    public Response MF2(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            logToConsole("reset");

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("reset_status_code", "0");
            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    /**
     * API alert on zipcode that is in alert state based on growth of postive cases. We define alert state as a growth of 2X over a 15 
     * second time interval. That is, if t0 - t14 there were 10 patients, and t15-t29 there were 25 patients, an alert state would be 
     * triggered.  However, if for time t15-t29 the new patient rate was 15, no alert would be generated.
     * @return ziplist = list of zipcodes in alert status
    */
    public Response RTR1(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            logToConsole("zipalertlist");

            ArrayList<String> ziplist = new ArrayList<>();

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("ziplist", gson.toJson(ziplist));
            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    /**
     * API alert on statewide when at least five zipcodes are in alert state (based on RT1) within the same 15 second window.
     * @param authKey
     * @return state_status = 0 = state is not in alert, 1 = state is in alert
     */
    public Response RTR2(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            logToConsole("alertlist");

            Boolean stateInAlert = false;

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("state_status", stateInAlert ? "1" : "0");
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/testcount")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    /**
     * API statewide positive and negative test counter
     * @param authKey
     * @return positive_test = count of positive test
     * @return negative_test = count of negative_test
     */
    public Response RTR3(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            logToConsole("testcount");

            Integer pos_test_count = 0;
            Integer neg_test_count = 0;

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("positive_test", pos_test_count.toString());
            responseMap.put("negative_test", neg_test_count.toString());
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/of1")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    /**
     * API to route pertson to the best fit hospital based on patient status, provider status, distance (zipcode). 
     * Continuously process incoming messages from incoming exchange.  Store results using a method that can be used 
     * to report routing path in subsequent APIs
     * @param authKey
     * @return
     */
    public Response OF1(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {
            logToConsole("of1");

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("reset_status_code", "0");
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    /**
     * API search by mrn patients location (home or specific hospital)
     * @param authKey
     * @return mrn = medical record number
     * @return location_code = hospital ID, ID = 0 for home assignment, ID=-1 for no assignment
     */
    public Response OF2(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("mrn") String mrn) {
        String responseString = "{}";
        try {
            logToConsole("getpatient");

            Launcher.edbEngine.assignPatient(mrn);
            // String mrn = "";
            Integer locationCode = -1;

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("mrn", mrn);
            responseMap.put("location_code", locationCode.toString());
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/gethospital/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    //TODO:
    /**
     * API to report hospital patient numbers
     * @param authKey
     * @return total_beds = total number of beds
     * @return avalable_beds = available number of beds
     * @return zipcode = zipcode of hospital
     */
    public Response OF3(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("id") String id) {
        String responseString = "{}";
        try {
            logToConsole("gethospital");

            Map<String, String> hospitalInfo = new HashMap<>();
            hospitalInfo = Launcher.edbEngine.getHospitalInfo(id);

            //generate a response
            responseString = gson.toJson(hospitalInfo);

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

}