# CS505_FinalProject

## Building and running package
The class VM has Java but not Maven, which is used for building the package.  

To install Maven run:  
```
sudo apt install maven -y
```

To build and start the program:  
```
./run.sh
```

Curl can be used to check that your API and database is responding:
*Note the use of a required security header
```
curl -H "X-Auth-API-Key: 12463865" http://localhost:9000/api/getteam
{"app_status_code":"0","team_name":"CEB: complex event brocessors","team_member_sids":"[\"1234\", \"1234\"]"}
```
---

## Creating a container to run the program

To compile the jar and build and run a container in the background:
```
./runDocker.sh
```    
    
