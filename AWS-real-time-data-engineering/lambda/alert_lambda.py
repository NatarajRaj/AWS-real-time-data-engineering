import json
import boto3

sns = boto3.client("sns")
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:ACCOUNT_ID:transaction-alerts"

def lambda_handler(event, context):
    for record in event["Records"]:
        payload = json.loads(record["body"])

        if payload["status"] == "FAILED" or payload["amount"] > 3000:
            message = f"""
ALERT 🚨
Transaction ID: {payload['transaction_id']}
Amount: {payload['amount']}
Status: {payload['status']}
"""

            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject="Transaction Alert"
            )

    return {"statusCode": 200}

// The Lambda code only publishes messages to SNS. Email and SMS delivery are configured through SNS subscriptions, not inside the Lambda code.
