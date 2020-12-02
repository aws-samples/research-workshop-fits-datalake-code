# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import boto3
from astropy.io import fits
from astropy.io.fits.verify import VerifyError
from urllib.parse import unquote_plus
import json

s3_client = boto3.client('s3')

# temp file used to process the header. The current limite is 500 MB.
tmpfile = "/tmp/tmp_fits.fits"

# 
def fits_header_extractor_handler(event, context):
    # get the name of the output bucket
    store_bucket = os.environ['FITSSTORE_BUCKET']

    #loop through the event records
    # event structure reference https://docs.aws.amazon.com/AmazonS3/latest/dev/notification-content-structure.html
    for record in event['Records']:
        if 'eventName' in record and record['eventName'].startswith('ObjectCreated'):
            response = createHandler(record, store_bucket)
        elif 'eventName' in record and record['eventName'].startswith('ObjectRemoved'):
            response = removeHandler(record, store_bucket)
        else:
            response = {"message" : "not a ObjectCreated or ObjectRemoved action. do nothing"}

    return {
        'message' : response
    }

def createHandler(record, store_bucket):
    # data.csv headers
    data ='hdu, source_bucket, source_key, card_name, card_value, card_comment, other' + os.linesep
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    # astropy doesn't have a method to open an S3 file, download to temp and process it
    s3_client.download_file(bucket, key, tmpfile)

    #loop through hdu list
    with fits.open(tmpfile) as hdul:
        i = 0
        for h in hdul:
            hdr = h.header
            for card_key in hdr.keys():
                # history card value is not serilizable
                if card_key == '' or card_key == 'HISTORY':
                    continue
                try:
                    card = [str(i), bucket, key]
                    card.append(card_key)
                    card.append(str(hdr[card_key]))
                    card.append('"'+hdr.comments[card_key]+'"')
                    card.append(os.linesep)
                    card_str = ','.join(card)
                    data += card_str
                except VerifyError as e:
                    print("verify error, ignore key {} error {}".format(card_key, e))
            i +=1
        # use source bucket name as prefix for output - the glue crawler will create the following partitions
        # partition 0: bucket name
        # partition 1: prefix
        # partition 2: fits file name
        response = s3_client.put_object(
            Body=str(data),
            Bucket=store_bucket,
            Key=bucket+'/'+key+'/data.csv' 
        )  

        return response

def removeHandler(record, store_bucket):
    # data.csv headers
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    response = s3_client.delete_object(
        Bucket=store_bucket,
        Key=bucket+'/'+key+'/data.csv' 
    )  

    return response
