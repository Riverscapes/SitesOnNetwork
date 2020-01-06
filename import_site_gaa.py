# Philip Bailey
# 3 Jan 2020
# Import CHaMP site generally available attributes (GAA) from CSV provided
# by Steve Fortney via email. Note he provided the file twice. Once with
# lots of visit level info and once with just the GAA columns.
# The attributes from the tab delimited CSV file were imported into the
# CHaMP_Sites table in the Workbench.
import sys
import csv
import os
import sqlite3
import argparse

fields = [
    ('ProgramSiteID', None),
    ('CategoryName', 'Category'),
    ('PanelName', 'Panel'),
    ('x_albers', "XAlbers"),
    ('y_albers', 'YAlbers'),
    ('Elev_M', 'Elevation'),
    ('Block', None),
    ('UseOrder', None),
    ('Sample', None),
    ('OwnerType', None),
    ('Strah', None),
    ('HUC4', None),
    ('HUC5', None),
    ('HUC6', None),
    ('LEVEL3_NM', 'Level3NM'),
    ('LEVEL4_NM', 'Level4NM'),
    ('CEC_L1', 'CECL1'),
    ('CEC_L2', 'CECL2'),
    ('ValleyClass',None),
    ('ChannelType', None),
    ('Ppt', None),
    ('CUMDRAINAG', None),
    ('DisturbedClassName', None),
    ('DisturbedClassCode', None),
    ('DistPrin1', None),
    ('NatClassName', None),
    ('NatClassCode', None),
    ('NatPrin1', None),
    ('NatPrin2', None),
    ('MeanU', None),
    ('PrimaryBedformClass', None)
]

def import_gaa(csvfile, database):

    cols = None
    site_values = {}

    with open(csvfile, 'r') as fieldCSVFile:
        reader = csv.reader(fieldCSVFile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            if not cols:
                cols = row
            else:
                site_name = row[cols.index('SiteName')]

                if site_name in site_values:
                    continue

                values = []
                for c, d in fields:
                    values.append(row[cols.index(c)])
                values.append(site_name)
                site_values[site_name] = values

    print('here')

    conn = sqlite3.connect(database)
    curs = conn.cursor()

    try:
        com = 'UPDATE CHaMP_Sites SET {0} WHERE SiteName = ?'.format(', '.join(['{0} = ?'.format(col[0] if not col[1] else col[1] ) for col in fields]))
        for site_name, values in site_values.items():
            curs.execute(com, values)

        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

def main():
    # parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', type=argparse.FileType('r'),  help='CSV file containing site GAAs')
    parser.add_argument('database', type=argparse.FileType('r'),  help='CHaMP Workbench database')
    args = parser.parse_args()

    try:
        import_gaa(args.csv.name, args.database.name)

    except AssertionError as e:
        print ("Assertion Error", e)
        sys.exit(1)
    except Exception as e:
        print('Unexpected error: {0}'.format(sys.exc_info()[0]), e)
        raise
        sys.exit(1)

if __name__ == '__main__':
    main()
