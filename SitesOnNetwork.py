import argparse
import sys
import os
import re
import json
import csv
from lib.shapefileloader import Shapefile
from lib.sitkaAPI import *
import ogr
from shapely.geometry import Point, mapping
from lib.env import setEnvFromFile
import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime

def CreateSiteMetricsProject(dirPath, metricSchemaName):
    """
    Create a CHaMP Site Metrics project, including the point ShapeFile and project file
    :param dirPath: Directory where the project will be placed. Must exist already.
    :param metricSchemaName: Name of the metric schema that will be downloaded and used.
    :return: None
    """

    if not os.path.isdir(dirPath):
        raise 'The output directory path does not exist'

    realizationDir = os.path.join(dirPath, 'Outputs')
    if not os.path.isdir(realizationDir):
        os.makedirs(realizationDir)

    metricsShp = os.path.join(realizationDir, 'TopoMetrics.shp')
    metricsCSV = os.path.join(realizationDir, 'Metrics.csv')

    # Download the metric values and generate the shapefile and CSV file
    SitesOnANetwork(metricsShp, metricsCSV, metricSchemaName)

    # Create a project.rs.xml file for the project
    SitesOnNetworkProject(dirPath, metricSchemaName, metricsShp, metricsCSV)

def SitesOnANetwork(shpPath, metricCSVPath, metricSchemaName):
    """
    Download metric values for a schema and write them to a ShapeFile and CSV file
    :param shpPath: Absolute path where the ShapeFile will get put. Must not exist already.
    :param metricCSVPath: Absolute path where the metrics will get written as a CSV
    :param metricSchemaName: Name of the metric schema to download
    :return: None
    """

    featuredict = {}
    setEnvFromFile(os.path.join(os.path.dirname(__file__), '.env'))

    # Retrieve the WGS84 spatial reference for geographic coordinates (lat/long)
    # http://spatialreference.org/ref/epsg/wgs-84/
    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(4326)

    outShape = Shapefile()
    outShape.create(shpPath, dest_srs, geoType=ogr.wkbPoint)

    # Get all the metrics. Using this call get the structure and data in one fell swoop
    metrics = rawCall('Visit/metricschemas/' + metricSchemaName + '/metrics')

    # Make shp fields for each metric in our schema
    namevalmap = {
        'SiteName': 'SiteName',
        'StreamName': 'StreamName',
        'Watershed': 'Watershed',
        'Latitude': 'Latitude',
        'Longitude': 'Longitude',
        'Year': 'Year',
        'VisitID': 'VisitID',
    }
    outShape.createField('ID', ogr.OFTInteger)
    outShape.createField('SiteName', ogr.OFTString)
    outShape.createField('StreamName', ogr.OFTString)
    outShape.createField('Watershed', ogr.OFTString)
    outShape.createField('Latitude', ogr.OFTReal)
    outShape.createField('Longitude', ogr.OFTReal)
    outShape.createField('Year', ogr.OFTInteger)
    outShape.createField('VisitID', ogr.OFTInteger)

    # Now let's pull the metric definition out of the massive "metrics" API call
    for attr in metrics[0]['values']:
        # We need to handle the 10 character limit explicitly because OGR does it automatically but
        # Doesn't return what it does. Thanks OGR!!!!

        fieldname = str(attr['name'][:10])
        counter = 1
        while fieldname in namevalmap.itervalues():
            nchars = len(str(counter))
            fieldlen = 9 - nchars
            fieldname = "{}_{}".format(fieldname[:fieldlen], counter)
            counter += 1

        namevalmap[attr['name']] = fieldname

        if attr['type'] == 'String':
            type = ogr.OFTString
        else:
            type = ogr.OFTReal

        # Create a field in the shapefile with the right type
        outShape.createField(fieldname, type)

    # Output the names to a CSV file so we can find them later
    fieldCSV = os.path.splitext(shpPath)[0] + "_fields.csv"
    with open(fieldCSV, 'wb') as fieldCSVFile:
        csvwriter = csv.writer(fieldCSVFile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(["METRICNAME", "SHPNAME"])
        for k,v in namevalmap.iteritems():
            csvwriter.writerow([k,v])

    # All watershes gives us waterhsed name and watershed url
    print "Getting all watersheds..."
    watersheds = rawCall("watersheds")
    shedurl = { ws['url']: ws['name'] for ws in watersheds }

    # Get all the sites
    print "Getting all Sites..."
    sites = rawCall("sites")

    # Each site give us year and watershed url
    # TODO: For testing I'm just going to process 5 dots on the map. REMOVE "DEBUGCOUNTER" Lines when you're ready for a full run
    # DEBUGCOUNTER = 0 # REMOVE ME
    for site in sites:
        siteobj = rawCall(site['url'], absolute=True)
        if not 'visits' in siteobj:
            print "    Skipping site {0} in watershed {1}".format(site['name'], site['watershedUrl'])
        else:
            for visit in siteobj['visits']:
                # Some visits have no sample year. Either because they haven't been sampled or the iPad not uploaded
                if visit['sampleYear']:
                    print "    Processing Visit: {}".format(visit['id'])
                    try:
                        featuredict[visit['id']] = {
                            'geometry': Point(float(siteobj['longitude']), float(siteobj['latitude'])),
                            'fields': {
                                'SiteName': siteobj['name'],
                                'StreamName' : siteobj['locale'],
                                'Watershed': shedurl[siteobj['watershedUrl']] if siteobj['watershedUrl'] in shedurl else "",
                                'Latitude': float(siteobj['latitude']),
                                'Longitude': float(siteobj['longitude']),
                                'Year': int(visit['sampleYear']),
                                'VisitID': int(visit['id']),
                            }
                        }
                    except Exception, e:
                        # TODO: Right now this throws out a lot of exceptions. Probably related to missing sampleYear
                        print "ERROR: Problem with site object: {}".format(e.message)
                        print visit
                        print siteobj
                else:
                    print "    Skipping Visit without sample year: {}".format(visit['id'])

        #     DEBUGCOUNTER += 1 # REMOVE ME
        # if DEBUGCOUNTER > 5: # REMOVE ME
        #     break # REMOVE ME

    # Store all the visit metrics in an easy-to-find dictionary
    for mobj in metrics:
        vid = mobj['itemUrl'].split('/')[-1]
        if len(vid) > 0 and vid.isdigit():
            if int(vid) in featuredict:
                for met in mobj['values']:
                    featuredict[int(vid)]['fields'][met['name']] = met['value']

    # Now it's time to write the shapefile:
    id = 1
    for vid, featObj in featuredict.iteritems():

        featureDefn = outShape.layer.GetLayerDefn()
        outFeature = ogr.Feature(featureDefn)
        ogrPolygon = ogr.CreateGeometryFromJson(json.dumps(mapping(featObj['geometry'])))
        outFeature.SetGeometry(ogrPolygon)
        outFeature.SetField('ID', id)

        for fieldName, fieldValue in featObj['fields'].iteritems():
            outFeature.SetField(namevalmap[fieldName], fieldValue)

        outShape.layer.CreateFeature(outFeature)
        id += 1

    # Now it's time to write the CSV file with metric values
    metricCSV = metricCSVPath
    with open(metricCSV, 'wb') as metricCSVFile:
        csvwriter = csv.DictWriter(metricCSVFile, delimiter=',', quoting=csv.QUOTE_MINIMAL, fieldnames=featuredict[featuredict.keys()[0]]['fields'].keys())
        csvwriter.writeheader()

        for vid, featObj in featuredict.iteritems():
            csvwriter.writerow(featObj['fields'])

def SitesOnNetworkProject(dirPath, metricSchemaName, shpPath, csvPath):
    """
    Create a Sites on Network riverscapes project file
    :param dirPath: Directory where the project file will get created. Must exist already.
    :param metricSchemaName: Name of the metric schema being downloaded.
    :param shpPath: Absolute path to the ShapeFile containing metrics
    :param csvPath: Absolute path to the CSV file containing metrics.
    :return: None
    """

    tree = ET.ElementTree(ET.Element('Project'))

    # Project level attributes
    tree.getroot().set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    tree.getroot().set("xsi:noNamespaceSchemaLocation",
                     "https://raw.githubusercontent.com/Riverscapes/Program/master/Project/XSD/V1/Project.xsd")

    ET.SubElement(tree.getroot(), 'Name').text ='CHaMP Site Metrics'
    ET.SubElement(tree.getroot(), 'ProjectType').text = 'CHaMP Metrics'

    nodMeta = ET.SubElement(tree.getroot(), 'MetaData')
    ET.SubElement(nodMeta, 'Meta', attrib={'name': 'Region'}).text = 'CRB'
    ET.SubElement(nodMeta, 'Meta', attrib={'name': 'DateCreated'}).text = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ET.SubElement(tree.getroot(), 'Inputs')

    nodRealizations = ET.SubElement(tree.getroot(), 'Realizations')
    nodMetrics = ET.SubElement(nodRealizations, 'Metrics', attrib={'dateCreated' : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    ET.SubElement(nodMetrics, 'Name').text = 'CHaMP Site Metrics'

    nodAnalyses = ET.SubElement(nodMetrics, 'Analyses')
    nodAnalysis = ET.SubElement(nodAnalyses, 'Analysis')

    ET.SubElement(nodAnalysis, 'Name').text = 'Topo Metrics'

    nodOutputs = ET.SubElement(nodAnalysis, 'Outputs')
    nodVector = ET.SubElement(nodOutputs, 'Vector')
    ET.SubElement(nodVector, 'Name').text = 'Topo Metrics ShapeFile'
    ET.SubElement(nodVector, 'Path').text = shpPath.replace(dirPath + os.sep,'')

    nodCSV = ET.SubElement(nodOutputs, 'CSV')
    ET.SubElement(nodCSV, 'Name').text = 'Topo Metrics CSV'
    ET.SubElement(nodCSV, 'Path').text = csvPath.replace(dirPath + os.sep, '')

    projectXMLPath = os.path.join(dirPath, 'project.rs.xml')
    rough_string = ET.tostring(tree.getroot(), 'utf-8')
    reparsed = xml.dom.minidom.parseString(rough_string)
    pretty = reparsed.toprettyxml(indent="\t")
    f = open(projectXMLPath, "wb")
    f.write(pretty)
    f.close()

def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('outdir',
                        type=str,
                        help='output directory')

    parser.add_argument('metricschema', type=str, help='metric schema name')

    parser.add_argument('--logfile',
                        type=str,
                        help='write the output of this script to a file')
    args = parser.parse_args()

    try:
        CreateSiteMetricsProject(args.outdir, args.metricschema)

    except AssertionError as e:
        print "Assertion Error", e
        sys.exit(1)
    except Exception as e:
        print 'Unexpected error: {0}'.format(sys.exc_info()[0]), e
        raise
        sys.exit(1)

"""
This handles the argument parsing and calls our main function
If we're not calling this from the command line then
"""
if __name__ == '__main__':
    main()
