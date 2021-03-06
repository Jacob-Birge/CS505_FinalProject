package final_project.httpcontrollers;

import com.google.gson.Gson;
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
    public Response MF1() {
        String responseString = "{}";
        try {
            logToConsole("getteam");
            
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "CEB: complex event brocessors");
            responseMap.put("team_member_sids", "[\"12257232\", \"12142047\"]");
            responseMap.put("app_status_code",Launcher.app_status_code.toString());

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
    public Response MF2() {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        try {
            logToConsole("reset");
            String code = "0";
            try{
                if (Launcher.ableToReset && Launcher.reset()){
                    code = "1";
                }
            } catch (Exception ex){}
            //generate a response
            responseMap.put("reset_status_code", code);
            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            responseMap.put("reset_status_code", "0");
            responseString = gson.toJson(responseMap);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    /**
     * API alert on zipcode that is in alert state based on growth of postive cases. We define alert state as a growth of 2X over a 15 
     * second time interval. That is, if t0 - t14 there were 10 patients, and t15-t29 there were 25 patients, an alert state would be 
     * triggered.  However, if for time t15-t29 the new patient rate was 15, no alert would be generated.
     * @return ziplist = list of zipcodes in alert status
    */
    public Response RTR1() {
        String responseString = "{}";
        try {
            logToConsole("zipalertlist");

            ArrayList<String> ziplist = Launcher.filterAlertZips();

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
    /**
     * API alert on statewide when at least five zipcodes are in alert state (based on RT1) within the same 15 second window.
     * @param authKey
     * @return state_status = 0 = state is not in alert, 1 = state is in alert
     */
    public Response RTR2() {
        String responseString = "{}";
        try {
            logToConsole("alertlist");

            ArrayList<String> ziplist = Launcher.filterAlertZips();

            Boolean stateInAlert = (ziplist.size() >= 5);

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
    /**
     * API statewide positive and negative test counter
     * @param authKey
     * @return positive_test = count of positive test
     * @return negative_test = count of negative_test
     */
    public Response RTR3() {
        String responseString = "{}";
        try {
            logToConsole("testcount");

            Long pos_test_count = Launcher.posCount;
            Long neg_test_count = Launcher.negCount;

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
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    /**
     * API search by mrn patients location (home or specific hospital)
     * @param authKey
     * @return mrn = medical record number
     * @return location_code = hospital ID, ID = 0 for home assignment, ID=-1 for no assignment
     */
    public Response OF2(@PathParam("mrn") String mrn) {
        String responseString = "{}";
        try {
            logToConsole("getpatient");

            String hospitalID = Launcher.edbEngine.getPatientLocation(mrn);

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("mrn", mrn);
            responseMap.put("location_code", hospitalID);
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
    /**
     * API to report hospital patient numbers
     * @param authKey
     * @return total_beds = total number of beds
     * @return avalable_beds = available number of beds
     * @return zipcode = zipcode of hospital
     */
    public Response OF3(@PathParam("id") String id) {
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