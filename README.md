Commandes succéssives à réaliser pour executer les jars :

docker build -t tp_big_data_img ./deploy 
docker run -it --rm -p 8088:8088 -p 9870:9870 -p 9864:9864 -v "$(pwd)\data:/data" -v "$(pwd)\jars:/jars" --name tp_big_data_container tp_big_data_img
docker exec -it tp_big_data_container  bash  

JOB 1 :
hdfs dfs -put /data/relationships/data.txt /data/
hadoop jar jars/tpfinal_etienne_gauge_job1.jar /data/data.txt /output/job1/ 
hdfs dfs -cat /output/job1/part-r-*

Job2:
hadoop jar jars/tpfinal_etienne_gauge_job2.jar /output/job1/  /output/job2/ 
hdfs dfs -cat /output/job2/part-r-*

Job3:
hadoop jar jars/tpfinal_etienne_gauge_job3.jar /output/job2/  /output/job3/ 
hdfs dfs -cat /output/job3/part-r-*
