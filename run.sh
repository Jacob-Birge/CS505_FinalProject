clear
mvn clean package -q
cd target
java -Djava.net.preferIPv4Stack=true -jar final_project-1.0-SNAPSHOT.jar