import boto3

s3 = boto3.resource('s3', 
    aws_access_key_id='AKIATOJJTWAA4R4N2M7N', 
    aws_secret_access_key='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' 
) 

bucket = s3.Bucket("rindner-bucket") 

dyndb = boto3.resource('dynamodb', 
    region_name='us-east-2', 
    aws_access_key_id='AKIATOJJTWAA4R4N2M7N', 
    aws_secret_access_key='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'
) 

table = dyndb.Table("DataTable") 

response = table.get_item( 
    Key={ 
        'PartitionKey': '1', 
        'RowKey': '-1' 
    } 
) 
item = response['Item'] 
print(item) 
