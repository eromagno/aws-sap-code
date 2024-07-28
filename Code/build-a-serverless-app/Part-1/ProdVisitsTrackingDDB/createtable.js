import { CreateTableCommand } from "@aws-sdk/client-dynamodb";
import { ddbClient } from "./ddbClient.js";

export const params = {
    AttributeDefinitions: [
      {
        AttributeName: "ProductVisitKey",
        AttributeType: "S",
      }      
    ],
    KeySchema: [
      {
        AttributeName: "ProductVisitKey",
        KeyType: "HASH",
      }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1,
    },
    TableName: "ProductVisits",
    StreamSpecification: {
      StreamEnabled: false,
    },
};

export const run = async () => {
    try {
        const data = await ddbClient.send(new CreateTableCommand(params));
        console.log("Table Created", data);
    } catch (error) {
        console.log("Error", error);
    }
}
run();