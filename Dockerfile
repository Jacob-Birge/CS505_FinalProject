FROM openjdk:8
RUN mkdir /myapp
COPY target/final_project-1.0-SNAPSHOT.jar /myapp
WORKDIR /myapp
CMD ["java", "-jar","final_project-1.0-SNAPSHOT.jar"]