'use strict'

const AWS = require('aws-sdk')
const uuidv4 = require('uuid/v4')

AWS.config.update({ region: process.env.Region, apiVersion: '2012-08-10' })

const s3 = new AWS.S3();

module.exports.handler = async (event) => {
    
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

        let s3KeyPrefix = `data/${year}/${month}/${day}/${hour}/${CustomerId}/${ProductId}/`
    
        const s3Key = `${s3KeyPrefix}${ProductVisitKey}`;
        console.log('Content uploaded to S3 to create CSV file.', s3Key);
        var s3Path = await fileUpload(csvContent, s3Key);
    
        console.log('Record saved as ', s3Path);
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
    Bucket: 'product-visits-datalake',
    Key: `${fileS3Key}.${type}`, // type is not required
    Body: buff,
    ContentEncoding: 'utf8', // required
    ContentType: `text/${type}` // required. Notice the back ticks
  }

  let location = '';
  let key = '';
  try {
    const { Location, Key } = await s3.upload(params).promise();
    location = Location;
    key = Key;
  } catch (error) {
    console.error(error.message);
    console.log(error)
  }
  
  console.log(location, key);
  
  return location;
  
}