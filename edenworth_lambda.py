# eden worth parser

import json
import datetime
import time
import boto3

dynamodb = boto3.resource('dynamodb')

print('Loading function')

def lambda_handler(event, context):
    
    print("Received event: " + json.dumps(event, indent=2))
    
    deviceid = json.dumps(event['device']).strip('"')                   # device id
    data = json.dumps(event['data']).strip('"')                         # hex payload from device
    message_timestamp = json.dumps(event['time']).strip('"')            # message timestamp
    message_datetime = datetime.datetime.utcfromtimestamp(int(message_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    snr = json.dumps(event['snr']).strip('"')                           # snr of message
    station = json.dumps(event['station']).strip('"')                   # base station of message
    avgsnr = json.dumps(event['avgSnr']).strip('"')                     # average snr
    lat = json.dumps(event['lat']).strip('"')                           # base station lat
    lng = json.dumps(event['lng']).strip('"')                           # base station lng
    rssi = json.dumps(event['rssi']).strip('"')                         # rssi of message
    seqnumber = json.dumps(event['seqNumber']).strip('"')               # seq number of message
    timestamp = int(time.time() * 1000)                                 # created at timestamp
    
    temperature_hex = "0x" + data[5:8]
    temperature = float.fromhex(temperature_hex)/100
    
    battery_hex = "0x" + data[15:18]
    battery = float.fromhex(battery_hex)/1000

    # print(temperature)
    # print(battery)
    
    table = dynamodb.Table('sigfox_edenworth')

    item = {
        'deviceid': str(deviceid),
        'data': str(data),
        'message_timestamp': str(message_timestamp),
        'message_datetime': str(message_datetime),
        'snr': str(snr),
        'station': str(station),
        'avgsnr': str(avgsnr),
        'lat': str(lat),
        'lng': str(lng),
        'rssi': str(rssi),
        'seqnumber': str(seqnumber),
        'temperature': str(temperature),
        'battery': str(battery),
        'timestamp': str(timestamp)
    }
    
    # write the todo to the database
    table.put_item(Item=item)
    
    # create a response
    response = {
        "statusCode": 200,
        "body": json.dumps(item)
    }

    return response
