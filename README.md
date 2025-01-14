JOB 1 :
docker build -t tp_big_data_img ./deploy 
docker run -it --rm -p 8088:8088 -p 9870:9870 -p 9864:9864 -v "C:\Users\etien\TP-Big-Data\hadoop-tp3\data:/data" -v "C:\Users\etien\TP-Big-Data\hadoop-tp3\jars:/jars" --name tp_big_data_container tp_big_data_img  
                                         
docker exec -i tp_big_data_container  bash                                                                                                                             

Dans le Job1 : mvn clean install 

Dans le bash : 

hdfs dfs -put /data/relationships/data.txt /data/

(hadoop fs -rm -r /output/job1) Si besoin de supprimer l'ancien dossier

hadoop jar jars/hadoop-tp3-collaborativeFiltering-job1-1.0.jar /data/data.txt /output/job1/ 

hdfs dfs -cat /output/job1/part-r-*![image](https://github.com/user-attachments/assets/eb941983-69c2-48a7-9b37-0d5bafc2e710)
