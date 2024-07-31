'use strict'
import { sqsClient } from "./sqsClient.js";
import { SendMessageCommand } from "@aws-sdk/client-sqs";

export const handler = async function(event) {

  console.log('The event: ',  JSON.stringify(event, null, 2));
  console.log('The event.body: ',  JSON.stringify(event.body, null, 2));
  
  try {

    var params = {
      QueueUrl: process.env.SQS_QUEUE_URL,  // QueueUrl: "https://sqs.us-east-1.amazonaws.com/827216809571/ProductVisitDataQueue"
      MessageBody: event.body,
      DelaySeconds: 10

    };

    const data = await sqsClient.send(new SendMessageCommand(params));
    console.log("Success when sending SQS message", data);

    return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'success', messageId: data.MessageId })
      }
    } catch (err) {
        console.error(err)
        return {
            statusCode: 500,
            headers: { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' },
            status: 'error',
            message: err
        }
    }
}