import json
import boto3
import os
import time

sf = boto3.client("stepfunctions")
ddb = boto3.client("dynamodb")

LOCK_TABLE = "airport-intelligence-pipeline-lock"
PIPELINE_NAME = "airport-intelligence-pipeline"

def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    now = int(time.time())

    # -------------------------------
    # STEP 1: Acquire execution lock
    # -------------------------------
    try:
        ddb.put_item(
            TableName=LOCK_TABLE,
            Item={
                "pipeline_name": {"S": PIPELINE_NAME},
                "ttl": {"N": str(now + 1800)}  # 30 min safety TTL
            },
            ConditionExpression="attribute_not_exists(pipeline_name)"
        )
        print("Lock acquired")

    except ddb.exceptions.ConditionalCheckFailedException:
        print("Pipeline already running. Skipping execution.")
        return {
            "status": "skipped",
            "reason": "pipeline already running"
        }

    # -------------------------------
    # STEP 2: Extract S3 info (if any)
    # -------------------------------
    input_payload = {"trigger": "s3"}

    if "Records" in event:
        record = event["Records"][0]
        input_payload.update({
            "bucket": record["s3"]["bucket"]["name"],
            "key": record["s3"]["object"]["key"]
        })

    # -------------------------------
    # STEP 3: Start Step Function
    # -------------------------------
    response = sf.start_execution(
        stateMachineArn=os.environ["STEP_FUNCTION_ARN"],
        input=json.dumps(input_payload)
    )

    print("Step Function started:", response["executionArn"])

    return {
        "status": "started",
        "executionArn": response["executionArn"]
    }
