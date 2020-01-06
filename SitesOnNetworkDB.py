import argparse
import sys
import os
import re
import json
import csv
from lib.shapefileloader import Shapefile
import ogr
from shapely.geometry import Point, mapping
import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime
import sqlite3

site_fields = {
    'Watershed': 'WatershedName',
    'Site': 'SiteName',
    'Stream': 'StreamName',
    'UTMZone': None,
    'UC_Chin': None,
    'SN_Chin': None,
    'LC_Steel': None,
    'MC_Steel': None,
    'UC_Steel': None,
    'SN_Steel': None,
    'Latitude': None,
    'Longitude': None,
    'VisitID': None,
    'VisitYear': None,
    'IsPrimary': None,
    'QCVisit': None,
    'PanelName': None,
    'VisitPhase': None,
    'VStatus': 'VisitStatus',
    'FishData': 'HasFishData',
    'Discharge': None,
    'D84': None,
    'ProgID':  'ProgramSiteID',
    'Category':  None,
    'Panel':  None,
    'XAlbers':  None,
    'YAlbers':  None,
    'Elevation':  None,
    'Block':  None,
    'UseOrder':  None,
    'Sample':  None,
    'OwnerType':  None,
    'Strah':  None,
    'HUC4':  None,
    'HUC5':  None,
    'HUC6':  None,
    'Level3NM':  None,
    'Level4NM':  None,
    'CECL1':  None,
    'CECL2':  None,
    'ValleyClss':  'ValleyClass',
    'ChannelTyp':  'ChannelType',
    'Ppt':  None,
    'DistClass':  'DisturbedClassName',
    'DistClssCd':  'DisturbedClassCode',
    'DistPrin1':  None,
    'NatClss':  'NatClassName',
    'NattClssCd':  'NatClassCode',
    'NatPrin1':  None,
    'NatPrin2':  None,
    'MeanU':  None,
    'PriBedClss':  'PrimaryBedformClass',
    'CUMDRAINAG':  None
}

def CreateSiteMetricsProject(dirPath, database, metricSchemaName):
    """
    Create a CHaMP Site Metrics project, including the point ShapeFile and project file
    :param dirPath: Directory where the project will be placed. Must exist already.
    :param database: CHaMP Workbench SQLite database containing the metric values.
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
    SitesOnANetwork(metricsShp, metricsCSV, database, metricSchemaName)

    # Create a project.rs.xml file for the project
    SitesOnNetworkProject(dirPath, metricSchemaName, metricsShp, metricsCSV)

def SitesOnANetwork(shpPath, metricCSVPath, database, metricSchemaName):
    """
    Download metric values for a schema and write them to a ShapeFile and CSV file
    :param shpPath: Absolute path where the ShapeFile will get put. Must not exist already.
    :param metricCSVPath: Absolute path where the metrics will get written as a CSV
    :param database: CHaMP Workbench SQLite database that contains the metrics.
    :param metricSchemaName: Name of the metric schema to download.
    :return: None
    """

    visits = {}
    shp_fields = {}
    metric_names = {}

    conn = sqlite3.connect(database)
    conn.row_factory = dict_factory
    curs = conn.cursor()

    # Load all the metric definitions for the specified schema
    metrics = {}
    unique_metrics = []
    curs.execute('SELECT MD.MetricID, MD.Title, DisplayNameShort, DataTypeID' +
        ' FROM Metric_Definitions MD' +
        ' INNER JOIN Metric_Schema_Definitions MSD ON MD.MetricID = MSD.MetricID' +
        ' INNER JOIN Metric_Schemas S ON MSD.SchemaID = S.SchemaID' +
        ' WHERE S.Title = ?', [metricSchemaName])
    for row in curs.fetchall():
        name = row['DisplayNameShort']
        shp = name.replace('_', '')[0:min(10, len(name))]
        
        # Attempt to build a unique 10 character version of each metric display name short
        attempt = 1
        while shp in unique_metrics:
            shp = shp[:-len(str(attempt))] + str(attempt)
            attempt += 1
        unique_metrics.append(shp)

        metrics[row['MetricID']] = {
            'Name': name,
            'FullName': row['Title'],
            'ShapeFile': shp,
            'DataType' : ogr.OFTReal if row['DataTypeID'] == 10023 else ogr.OFTString
        }
    
    # Verify that the ShapeFile field names are unique
    shpfields = [metric['ShapeFile'] for id, metric in metrics.items()]
    print(len(shpfields), 'metrics loaded.')
    if len(shpfields) != len(set(shpfields)):
        raise 'Metric field names for the Shapefile are not unique'

    # Output the metric names to a CSV file for reference
    fieldCSV = os.path.splitext(shpPath)[0] + "_fields.csv"
    with open(fieldCSV, 'w') as fieldCSVFile:
        csvwriter = csv.writer(fieldCSVFile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(['MetricID', 'Metric Name', 'Display Name Short', 'ShapeFile Field Name'])
        for metricid, metric in metrics.items():
            csvwriter.writerow([metricid, metric['FullName'], metric['Name'], metric['ShapeFile']])

    # Load all the visits with their site information
    curs.execute('SELECT *' +
        ' FROM CHaMP_Visits V' +
        ' INNER JOIN CHaMP_Sites S ON V.SiteID = S.SiteID' +
        ' INNER JOIN CHaMP_Watersheds W ON S.WatershedID = W.WatershedID'
        ' WHERE (Latitude IS NOT NULL) AND (Longitude IS NOT NULL) AND W.WatershedID NOT IN (99, 102, 104, 27)')
    for row in curs.fetchall():
        visitid = row['VisitID']
        visits[visitid] = {}
        for shpfield, dbfield in site_fields.items():
            if not dbfield:
                dbfield = shpfield
            visits[visitid][shpfield] = row[dbfield]

            if shpfield not in shp_fields and row[dbfield]:
                if isinstance(row[dbfield], str):
                    shp_fields[shpfield] = ogr.OFTString
                elif isinstance(row[dbfield], int):
                    shp_fields[shpfield] = ogr.OFTInteger
                else:
                    shp_fields[shpfield] = ogr.OFTReal

    print(len(visits), 'visits retrieved from the database')        

    # Load all the metric values
    curs.execute('SELECT I.VisitID, MetricID, MetricValue' +
        ' FROM Metric_Schemas S' +
        ' INNER JOIN Metric_Batches B ON S.SchemaID = B.SchemaID' +
        ' INNER JOIN Metric_Instances I ON B.BatchID = I.BatchID' +
        ' INNER JOIN Metric_VisitMetrics VM ON I.InstanceID = VM.InstanceID'
        ' WHERE S.Title = "Final - Visit Metrics 2018_02_16"')
    for row in curs.fetchall():
        visitid = row['VisitID']
        metric  = metrics[row['MetricID']]
        visits[visitid][metric['ShapeFile']] = row['MetricValue']

    print('Metric values loaded from the database')

    # Retrieve the WGS84 spatial reference for geographic coordinates (lat/long)
    # http://spatialreference.org/ref/epsg/wgs-84/
    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(4326)

    outShape = Shapefile()
    outShape.create(shpPath, dest_srs, geoType=ogr.wkbPoint)
    [outShape.createField(field_name, field_type) for field_name, field_type in shp_fields.items()]
    [outShape.createField(metric['ShapeFile'], metric['DataType']) for metric in metrics.values()]

 
    fieldCSV = os.path.splitext(shpPath)[0] + ".csv"
    with open(fieldCSV, 'w') as fieldCSVFile:
        csvwriter = csv.writer(fieldCSVFile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        fields = list(shp_fields.keys())
        csvwriter.writerow(fields)
        for visitid, values in visits.items():
            csv_values = []
            for field in fields:
                csv_values.append(values[field] if field in values else None)
            csvwriter.writerow(csv_values)
            
    print('CSV file written to', fieldCSV)

    featureDefn = outShape.layer.GetLayerDefn()
    for visit in visits.values():

        outFeature = ogr.Feature(featureDefn)
        geom = Point(float(visit['Longitude']), float(visit['Latitude']))
        ogrPoint = ogr.CreateGeometryFromJson(json.dumps(mapping(geom)))
        outFeature.SetGeometry(ogrPoint)
        [outFeature.SetField(fieldName, fieldValue) for fieldName, fieldValue in visit.items() if fieldValue]
        outShape.layer.CreateFeature(outFeature)


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
    f = open(projectXMLPath, "w")
    f.write(pretty)
    f.close()

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('outdir', type=str, help='Riverscapes project output directory')
    parser.add_argument('database', type=argparse.FileType('r'), help='CHaMP workbench database path')
    parser.add_argument('metricschema', type=str, help='metric schema name')
    parser.add_argument('--logfile',  type=str,   help='write the output of this script to a file')
    args = parser.parse_args()

    try:
        CreateSiteMetricsProject(args.outdir, args.database.name, args.metricschema)

    except AssertionError as e:
        print("Assertion Error", e)
        sys.exit(1)
    except Exception as e:
        print('Unexpected error: {0}'.format(sys.exc_info()[0]), e)
        raise
        sys.exit(1)

if __name__ == '__main__':
    main()
