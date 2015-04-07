import argparse
from osti_record import OstiRecord

parser = argparse.ArgumentParser()
parser.add_argument("-n", default=5, type=int, help="number of materials to submit to OSTI")
args = parser.parse_args()

#osti = OstiRecord(['mp-4', 'mp-1986', 'mp-571567'])
osti = OstiRecord(n=args.n)
print osti.records_xml.toprettyxml()
