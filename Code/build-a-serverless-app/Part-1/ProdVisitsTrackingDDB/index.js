'use strict'

// AWS Lambda function:  productVisitsDataHandler
import { PutItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { ddbClient } from "./ddbClient.js";
import { v4 as uuidv4 } from 'uuid';

export const handler = async function(event) {
        
    console.log('The event: ',  JSON.stringify(event, null, 2));

    const allPromises = event.Records.map(async (record) => {
        const body = record.body;
        
        console.log('The body: ',  JSON.stringify(body, null, 2));

        try {
            const { ProductId, ProductName, Category, PricePerUnit, CustomerId, CustomerName, TimeOfVisit } = body;
            
            if (!ProductId || !ProductName || !Category || !PricePerUnit || !CustomerId || !CustomerName || !TimeOfVisit) {
                console.log('Please provide values for product, category, customer and time of visit.');

                throw new Error("Error incomplete mesasage format.")
            }

            // Create the unique visist Key
            body.ProductVisitKey = uuidv4();

            console.log(`${body.ProductVisitKey} -ProductId ${ProductId} ${ProductName} ${Category} ${PricePerUnit} ${CustomerId} ${CustomerName} ${TimeOfVisit}`);

            const params = {
                TableName: process.env.DYNAMODB_TABLE_NAME,
                Item: marshall(body || {})
              };

            const createResult = await ddbClient.send(new PutItemCommand(params));
            console.log(createResult);

            console.log('Product Visit record is successfully created.');

        } catch (err) {
            console.error(err.message);
            console.error(err);
        }

    });

    await Promise.all(allPromises);

    return {};
}