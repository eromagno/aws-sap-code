'use strict'
import { s3Client } from "./s3Client.js";
import { PutObjectCommand } from "@aws-sdk/client-s3";

export const handler = async function(event){
    
  console.log('Received event:', JSON.stringify(event, null, 2));
  
  const allPromises = event.Records.map(async (record) => {

    try {

      if (record.eventName == 'INSERT') {
        
        let ProductVisitKey = record.dynamodb.NewImage.ProductVisitKey.S;
        let ProductId = record.dynamodb.NewImage.ProductId.S;
        let ProductName = record.dynamodb.NewImage.ProductName.S;
        let Category = record.dynamodb.NewImage.Category.S;
        let PricePerUnit = record.dynamodb.NewImage.PricePerUnit.S; 
        let CustomerId = record.dynamodb.NewImage.CustomerId.S;
        let CustomerName = record.dynamodb.NewImage.CustomerName.S;
        let TimeOfVisit = record.dynamodb.NewImage.TimeOfVisit.S;

        let year = TimeOfVisit.split('T')[0].substr(0,4);
        let month = TimeOfVisit.split('T')[0].substr(5,2);
        let day = TimeOfVisit.split('T')[0].substr(8,2);
        let hour = TimeOfVisit.split('T')[1].split('.')[0].substr(0,2);

        console.log(`${ProductId} ${ProductName} ${Category} ${PricePerUnit} 
            ${CustomerId} ${CustomerName} ${TimeOfVisit}`);

        let row = [ProductVisitKey, ProductId, ProductName, Category, PricePerUnit, 
                    CustomerId, CustomerName, TimeOfVisit];

        let csvContent = row.join(',');

        console.log('Row content to create CSV: [', csvContent, ']' );

        let s3KeyPrefix = `data/${year}/${month}/${day}/${hour}/${CustomerId}/${ProductId}/`
    
        const s3Key = `${s3KeyPrefix}${ProductVisitKey}`;
        console.log('Ready to load s3Key = [', s3Key, ']');

        var s3Path = await fileUpload(csvContent, s3Key);
    
        console.log('Record saved as ', s3Path);
      }
      else {
        console.log('Product Visit record NOT  processed. eventName = [', record.eventName, ']' )

      }
      
      console.log('Product Visit record successfully processed.');

    } catch (err) {
      console.error(err.message);
      console.error(err);
    }

  });

  await Promise.all(allPromises);

  return {}
}



/**
 * @param  {string}  content Data
 * @param  {string}  content Data
 * @return {string}  file url
 */
const fileUpload = async (content, fileS3Key) => {
  
  // Ensure that it POST a base64 data to your server.

  let buff = Buffer.from(content, 'utf8');
  
  const type = 'csv';
  
  const params = {
    // Bucket: 'product-visits-datalake',
    Bucket: process.env.BUCKET_NAME,
    //Key: "test.csv",
    Key: `${fileS3Key}.${type}`, // type is not required
    Body: buff,
    ContentEncoding: 'utf8', // required
    ContentType: `text/${type}` // required. Notice the back ticks
  }

  let location = '';
  let key = '';
  let data;

  try {
    // const { Location, Key } = await s3Client.upload(params);
    data = await s3Client.send(new PutObjectCommand(params));
    console.log("Success", data);

    //location = Location;
    // key = Key;

  } catch (error) {
    console.error(error.message);
    console.log(error)
  }
  
  //console.log(location, key);
  
  // return location;
  return data;
  
}