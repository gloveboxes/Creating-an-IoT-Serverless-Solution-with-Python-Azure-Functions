import logging

import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService
# from azure.cosmosdb.table.models import Entity
# from azure.cosmosdb.table.tablebatch import TableBatch
import requests
import json
import os


deviceStateTable = "DeviceState"
calibrationTable = "Calibration"
geoLocation = "Sydney"

storageConnectionString = os.environ['StorageConnectionString']
partitionKey = os.environ['PartitionKey']
signalrUrl = os.environ['SignalrUrl']

table_service = TableService(connection_string=storageConnectionString)
if not table_service.exists(deviceStateTable):
    table_service.create_table(deviceStateTable)

calibrationDictionary = {}


def main(event: func.EventHubEvent):

    devid = event.enqueued_time

    stateUpdates = {}

    messages = json.loads(event.get_body().decode('utf-8'))

    for msg in messages:

        environment = {}

        parseTelemetry(msg, environment, partitionKey)

        calibrationData = getCalibrationData(
            calibrationDictionary, msg['deviceId'])

        if not calibrationData is None:
            environment["Celsius"] = calibrate(
                environment["Celsius"], calibrationData["TemperatureSlope"], calibrationData["TemperatureYIntercept"])
            environment["Humidity"] = calibrate(
                environment["Humidity"], calibrationData["HumiditySlope"], calibrationData["HumidityYIntercept"])
            environment["hPa"] = calibrate(
                environment["hPa"], calibrationData["PressureSlope"], calibrationData["PressureYIntercept"])

        stateUpdates[msg['deviceId']] = environment

    for item in stateUpdates:
        table_service.insert_or_replace_entity(deviceStateTable, environment)
        notifyClients(signalrUrl, environment)


def parseTelemetry(msg, environment, PartitionKey):
    global geoLocation

    environment['PartitionKey'] = PartitionKey
    environment['RowKey'] = msg['deviceId']
    environment['DeviceId'] = msg['deviceId']
    environment['Geo'] = geoLocation
    environment['Celsius'] = msg['temperature']
    environment['Humidity'] = msg['humidity']

    if 'pressure' in msg:
        environment['hPa'] = msg['pressure']

    environment['Id'] = msg['messageId']
    environment['Schema'] = 2


def notifyClients(signalrUrl, telemetry):
    headers = {'Content-type': 'application/json'}
    r = requests.post(signalrUrl, data=json.dumps(telemetry), headers=headers)
    logging.info(r)


def calibrate(value, slope, intercept):
    return value * slope + intercept


def getCalibrationData(calibrationDictionary, deviceId):
    if deviceId not in calibrationDictionary:
        try:
            calibrationDictionary[deviceId] = table_service.get_entity(
                calibrationTable, partitionKey, environment[deviceId])
        except:
            calibrationDictionary[deviceId] = None

    return calibrationDictionary[deviceId]
