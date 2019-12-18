import boto3
import logging
import random
import re
import time

logger= logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(event)
    
    # S3 FILE LOGIC
    files = []
    key = ""
    for record in event['Records']:
        bucket = record['s3']['bucket']['name'].replace("%3D", "=").replace("%2B", "+")
        key = record['s3']['object']['key'].replace("%3D", "=").replace("%2B", "+")
        files.append(bucket + '/' + key)
    
    # Grab exchange
    parse_key = re.search("trades.(.*).normalized", key)
    key_slice = parse_key.group(1)
    grab_exchange = re.search("(.*).normalized", key_slice)
    exchange = grab_exchange.group(1)
    
    # Grab dev or prod
    parse_env = re.search("topics/(.*).market.trades.{}.normalized/".format(exchange), key)
    env = parse_env.group(1)
    
    # EMR CLUSTER LOGIC
    conn = boto3.client("emr")
    clusters = conn.list_clusters()
    
    cluster_name = "Deequ-Mini-v003-prod" if env == "prod" else "Deequ-Mini-v003-dev"
    
    print(cluster_name)
    
    waiting_clusters = [c["Id"] for c in clusters["Clusters"]
                if c["Name"] == cluster_name and c["Status"]["State"] in ["WAITING"]]
                
    print("WAITING CLUSTERS: {}".format(waiting_clusters))
    
    running_clusters = [c["Id"] for c in clusters["Clusters"]
                if c["Name"] == cluster_name and c["Status"]["State"] in ["RUNNING"]]
                
    print("RUNNING CLUSTERS: {}".format(running_clusters))
    
    cluster_id = waiting_clusters[random.randrange(len(waiting_clusters))] if len(waiting_clusters) > 0 else running_clusters[random.randrange(len(running_clusters))]
    
    
    logger.info(cluster_id)
    
    if not cluster_id:
        sys.stderr.write("No valid clusters\n")
        sys.stderr.exit()
        
    sasl_username = "NH4IKJE6QIPNH4S5" if env == "prod" else "YUVUXPJM7KW4YCUO"
    sasl_password = "pz6Sqvs5LZxo70DNE5wqVstPzzw6BpV9J9rF/4zuElQTG+Q9JVMdiwT2fSq4gnT5" if env == "prod" else "lLyV94ko+zFchYOgMGIG+JJnwLwB/I8PC/+4OPzppst52xA0fruz+PaD7uM8LUnA"
    
    validation_step_args = [
        'spark-submit', 
        '--deploy-mode', 'cluster',
        '--class', 'com.digitalassetsdata.TradesValidation', 
        '--jars', 's3://rossi-working-virginia/deequ_deployment/deequ-1.0.1.jar,s3://rossi-working-virginia/deequ_deployment/kafka-clients-2.1.0.jar,s3://rossi-working-virginia/deequ_deployment/kafka_2.11-2.1.0.jar', 
        's3://rossi-working-virginia/deequ_deployment/deequ_sbt_2.11-0.1-SNAPSHOT.jar'
        ]
    validation_step_args.extend([sasl_username, sasl_password, exchange, env])
    validation_step_args.extend(files)
    
    logger.info(validation_step_args)

    # Validation step
    step = {"Name": "TradesValidation" + time.strftime("%Y%m%d-%H:%M"),
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": validation_step_args
            }
        }
    
    action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    
    return "Added step: %s"%(action)
