import requests
import os
import math
import sys
import json
from progressbar import ProgressBar
from userinput import query_yes_no

class Tokenator:
    """

    """
    TOKEN = None

    def __init__(self):
        if self.TOKEN is None:
            print "Getting security token"
            response = requests.post(os.environ.get('KEYSTONE_URL'), data={
                "username": os.environ.get('KEYSTONE_USER'),
                "password": os.environ.get('KEYSTONE_PASS'),
                "grant_type": "password",
                "client_id": os.environ.get('KEYSTONE_CLIENT_ID'),
                "client_secret": os.environ.get('KEYSTONE_CLIENT_SECRET'),
                "scope": 'keystone openid profile'
            })
            respObj = json.loads(response.content)
            Tokenator.TOKEN = "bearer " + respObj['access_token']
        else:
            print "reusing security token"

def getVisits():
    """
    Get all the instances we need to delete
    :param schemaName:
    :return:
    """
    tokenator = Tokenator()
    print "Getting visit data"
    url = "{0}/visits".format(os.environ.get('API_BASE_URL'))
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    respObj = json.loads(response.content)

    visits = {}
    for obj in respObj:
        visits[obj['id']] = obj

    return visits

def rawCall(url, absolute=False):
    tokenator = Tokenator()
    print "Making Call: {}".format(url)
    if absolute == False:
        url = "{0}/{1}".format(os.environ.get('API_BASE_URL'), url)

    retry = True
    retries = 0
    # Really basic retry functionality.
    # TODO: This catches "connection reset by peer" but not error status codes like 404 or 500
    while retry and retries < 10:
        try:
            retries += 1
            response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
            retry = False
        except Exception, e:
            print "ERROR: Problem with API Call: {}. retrying... {}".format(url, retries)
    respObj = json.loads(response.content)

    return respObj


def getSites():
    tokenator = Tokenator()
    print "Getting sites"
    url = "{0}/metricschemas".format(os.environ.get('API_BASE_URL'))
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    respObj = json.loads(response.content)

    return respObj

def getMetricSchema(SchemaName):
    tokenator = Tokenator()
    print "Getting sites"
    url = "{0}/sites".format(os.environ.get('API_BASE_URL'))
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    respObj = json.loads(response.content)

    return respObj

def getWatersheds():
    tokenator = Tokenator()
    print "Getting watersheds"
    url = "{0}/watersheds".format(os.environ.get('API_BASE_URL'))
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    respObj = json.loads(response.content)

    watersheds = {}
    for obj in respObj:
        response = requests.get(obj['url'], headers={"Authorization": tokenator.TOKEN})
        respObj = json.loads(response.content)
        watersheds[obj['name']] = [site['name'] for site in respObj['sites']]
        print "     Getting sites for watershed: {}".format(obj['name'])

    return watersheds

def downloadFile(url, localpath):
    tokenator = Tokenator()
    print "Getting visit file data"
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    with open(localpath, 'wb') as f:
        f.write(response.content)
        print "Downloaded file: {} to: {}".format(url, localpath)

def getVisitFieldFiles(visitID):
    """
    Get all the instances we need to delete
    :param schemaName:
    :return:
    """
    tokenator = Tokenator()
    print "Getting visit file data"
    url = "{0}/visits/{1}/fieldFolders".format(os.environ.get('API_BASE_URL'), visitID)
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    respObj = json.loads(response.content)

    files = {}
    counter = 0
    for folder in respObj:
        url = "{0}/visits/{1}/fieldFolders/{2}".format(os.environ.get('API_BASE_URL'), visitID, folder['name'])
        response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
        respObj = json.loads(response.content)
        files[folder['name']] = respObj
        counter += len(respObj)

    print "  -- Found {} field files for the visit {}".format(counter, visitID)
    return files


def getInstances(schemaName):
    """
    Get all the instances we need to delete
    :param schemaName:
    :return:
    """
    tokenator = Tokenator()
    print "Getting instances"
    url = "{0}/visit/metricschemas/{1}".format(os.environ.get('API_BASE_URL'), schemaName)
    response = requests.get(url, headers={"Authorization": tokenator.TOKEN})
    respObj = json.loads(response.content)
    print "  -- Found {} instances for the schema {}".format(len(respObj['instances']), schemaName)
    return [inst['url'] for inst in respObj['instances']]

def deleteInstances(instances):
    """

    :param instances:
    :return:
    """
    tokenator = Tokenator()
    counter = 0
    print "Deleting all {} existing instances:".format(len(instances))

    custom_options = {
        'start': 0,
        'end': 100,
        'width': 60,
        'blank': '_',
        'fill': '#',
        'format': '%(progress)s%% [%(fill)s%(blank)s]'
    }
    progbar = ProgressBar(**custom_options)

    try:
        for url in instances:
            response = requests.delete(url, headers={"Authorization": tokenator.TOKEN})
            respObj = json.loads(response.content)
            if response.status_code != 200:
                raise "FAILED with code: {} and error: '{}'".format(response.status_code, response.text)
            counter += 1
            progbar.progress = math.floor(float(counter) / float(len(instances)) * 100)
            sys.stdout.write("\r {} {} of {}".format(str(progbar), counter, len(instances)))

    except Exception, e:
        print " FAILED after deleting {} instances".format(counter)
        raise "FAIL"


    print "  -- deleted {} instances".format(len(instances))
    return [inst['url'] for inst in respObj['instances']]

def deleteSchema(schemaName):

    try:
        tokenator = Tokenator()
        instances = getInstances(schemaName)

        if not query_yes_no("\nIt's still not too late. Are you sure?"):
            return

        success = deleteInstances(instances)

        url = "{0}/visit/metricschemas/{1}".format(os.environ.get('API_BASE_URL'), schemaName)
        response = requests.delete(url, headers={"Authorization": tokenator.TOKEN})
        respObj = json.loads(response.content)
        print "Schema Deleted"
        return [inst['url'] for inst in respObj['instances']]

    except Exception, e:
        print "whoops"

    print "boop"
