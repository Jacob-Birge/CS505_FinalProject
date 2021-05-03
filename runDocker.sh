mvn clean package -q
sudo docker build . -t final_project
sudo docker run -d --name CEB --rm -p 9000:9000 final_project