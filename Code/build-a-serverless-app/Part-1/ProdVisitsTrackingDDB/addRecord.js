'use strict'
//import { GetItemCommand, ScanCommand, PutItemCommand, DeleteItemCommand, UpdateItemCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import { PutItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { ddbClient } from "./ddbClient.js";
import { v4 as uuidv4 } from 'uuid';


export const handler = async function(event) {
    
    console.log('Received event:', JSON.stringify(event, null, 2));

    const allPromises = event.Records.map(async (record) => {

        const body = record.body;
        
        console.log('The body: ',  JSON.stringify(body, null, 2));

        try {
            const { ProductId, ProductName, Category, PricePerUnit, CustomerId, CustomerName, TimeOfVisit } = body;
            
            if (!ProductId || !ProductName || !Category || !PricePerUnit || !CustomerId || !CustomerName || !TimeOfVisit) {
                console.log('Please provide values for product, category, customer and time of visit.');
                throw new Error("Error incomplete mesasage format.");

            }

            body.ProductVisitKey = uuidv4();

            console.log(`${body.ProductVisitKey} -ProductId ${ProductId} ${ProductName} ${Category} ${PricePerUnit} ${CustomerId} ${CustomerName} ${TimeOfVisit}`);

            const params = {
                // TableName: process.env.DYNAMODB_TABLE_NAME,
                TableName: 'ProductVisits',
                Item: marshall(body || {})
              };

            // await docClient.put(params).promise();
            const createResult = await ddbClient.send(new PutItemCommand(params));
            console.log(createResult);

            console.log('Product Visit record is successfully created.');

        } catch (err) {
            console.error(err.message);
            console.error(err);
        }

    });

    await Promise.all(allPromises);

    return {}
}

let event = {
    "Records": [
      {
        "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
        "receiptHandle": "MessageReceiptHandle",
        "body": 
        {
            "ProductId": "c96b49bb-c378-4a15-b2e3-842a9850b23d",
            "ProductName": "Gloves",
            "Category": "Accessories",
            "PricePerUnit": 10,
            "CustomerId": "be44af0a-74f9-438e-a3ac-e3e21d84259f",
            "CustomerName": "John Doe",
            "TimeOfVisit": "2023-01-31T16:23:42.389Z" 
        },
        "attributes": {
          "ApproximateReceiveCount": "1",
          "SentTimestamp": "1523232000000",
          "SenderId": "123456789012",
          "ApproximateFirstReceiveTimestamp": "1523232000001"
        },
        "messageAttributes": {},
        "md5OfBody": "{{{md5_of_body}}}",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
        "awsRegion": "us-east-1"
      },
      {
        "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cghs",
        "receiptHandle": "MessageReceiptHandle",
        "body": 
        {
            "ProductId": "c96b49bb-c378-4a15-b2e3-842a9850b23d",
            "ProductName": "Jacket",
            "Category": "Clothing",
            "PricePerUnit": 189,
            "CustomerId": "wf9s200a-72f4-42ge-pw0c-10f3jf03l59f",
            "CustomerName": "Paul Jones",
            "TimeOfVisit": "2023-02-15T19:13:32.314Z" 
        },
        "attributes": {
          "ApproximateReceiveCount": "1",
          "SentTimestamp": "1523232000000",
          "SenderId": "123456789012",
          "ApproximateFirstReceiveTimestamp": "1523232000001"
        },
        "messageAttributes": {},
        "md5OfBody": "{{{md5_of_body}}}",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
        "awsRegion": "us-east-1"
      }
    ]
  }

handler(event);