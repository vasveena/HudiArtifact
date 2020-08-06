import json
import boto3
import time

def lambda_handler(event, context):
    conn_emr = boto3.client('emr', region_name='us-east-1')
    cluster_id = "j-2L7W2L7QZF61X"
    
    #Read DynamoDB table that contains the RDS database and table information
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ChangeControlTable')
    response = table.scan()
    
    for data in response['Items']:
      #Get configurations from DynamoDB table
      databaseName = data['databaseName']
      tableName = data['tableName']
      recordKey = data['recordKey']
      partitionPath = data['partitionPath']
      sourceOrderField = data['sourceOrderField']
      sourceSchema = data['sourceSchema']
      targetSchema = data['targetSchema']
      
      # Submit step \
      step_args = ["/usr/bin/spark-submit", "--jars","hdfs:///apps/hudi/lib/*.jar",
                 "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
                 "--packages","org.apache.spark:spark-avro_2.11:2.4.4",
                 "--master","yarn","--deploy-mode","client",
                 "--conf","spark.kryoserializer.buffer.max=1024m",
                 "hdfs:///apps/hudi/lib/hudi-utilities-bundle.jar", 
                 "--table-type","COPY_ON_WRITE",
                 "--source-class","org.apache.hudi.utilities.sources.ParquetDFSSource",
                 "--source-ordering-field",sourceOrderField,
                 "--target-base-path","s3://vasveena-test-demo/output/"+databaseName+"/"+tableName,
                 "--transformer-class","org.apache.hudi.utilities.transform.AWSDmsTransformer",
                 "--payload-class","org.apache.hudi.payload.AWSDmsAvroPayload",
                 "--target-table",tableName,
                 "--props","s3://vasveena-test-demo/prop/dfs-source.properties",
                 "--schemaprovider-class","org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
                 "--hoodie-conf","hoodie.datasource.hive_sync.enable=true,hoodie.datasource.write.recordkey.field="+recordKey+",hoodie.datasource.write.partitionpath.field="+partitionPath+",hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator,hoodie.deltastreamer.source.dfs.root=s3://hudi-ds-demo-620614497509/dms-full-load-path/"+databaseName+"/"+tableName+",hoodie.deltastreamer.schemaprovider.source.schema.file="+sourceSchema+",hoodie.deltastreamer.schemaprovider.target.schema.file="+targetSchema+",hoodie.datasource.hive_sync.table="+tableName+",hoodie.datasource.hive_sync.database="+databaseName+",hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.NonPartitionedExtractor"]

      step = {"Name": "Change capture for DB: "+databaseName+" table: " +tableName+" on time: "+time.strftime("%Y-%m-%d-%H:%M"),
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': step_args
               }
            }
      action = conn_emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
      step_id = action['StepIds']
      print("Submitted step "+step_id[0]+" for DB: "+databaseName+" and table: "+tableName)
