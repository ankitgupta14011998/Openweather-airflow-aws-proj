# Openweather-airflow-aws-proj

### Steps
1. Create EC2 instance with type >=t2.small. provide IAM role to access S3 bucket.
2. install remote-SSH in vs-code, provide following info(instance name, host name, user, access key file loc).
3. use openweather_api access key to get data.
4. install venv in ec2-instance "python3 -m venv airflow_env" and start venv using "source airflow_env/bin/activate"
5. install apache-airlfow(may get sqlite3 version error, follow error log instructions to resolve, use super use if prompted for root access needed.)
6. run apache standalone server.
7. place the dag file in dag_config folder. The dag will automatically run when triggered.
8. use AWS key, secret key , s3fs module to export the data to s3 bucket.

follow this link for details <br>
https://www.youtube.com/watch?v=uhQ54Dgp6To&t=657s
