import logging
import argparse
from osti_record import OstiRecord, OstiMongoAdapter

parser = argparse.ArgumentParser()
parser.add_argument("--log", help="show log output", action="store_true")
parser.add_argument("--prod", action="store_true", help="""use production DB.""")
group = parser.add_mutually_exclusive_group()
group.add_argument("-n", default=0, type=int, help="""number of materials to
                    submit to OSTI. The default (0) collects all materials not
                    yet submitted.""")
group.add_argument('-l', nargs='+', type=int, help="""list of material id's to
                    submit. mp-prefix internally added, i.e. use `-l 4 1986
                   571567`.""")
group.add_argument("--reset", action="store_true", help="""reset collections""")
group.add_argument("--info", action="store_true", help="""retrieve materials
                   already having a doi saved in materials collection""")
group.add_argument("--plotly", action="store_true", help="""init plotly graph""")
args = parser.parse_args()

loglevel = 'DEBUG' if args.log else 'WARNING'
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('mg.build.osti_doi')
logger.setLevel(getattr(logging, loglevel))

db_yaml = 'materials_db_{}.yaml'.format('prod' if args.prod else 'dev')
print db_yaml
if args.reset or args.info or args.plotly:
    matad = OstiMongoAdapter.from_config(db_yaml=db_yaml)
    if args.reset:
        matad._reset()
    elif args.info:
        print '{} DOIs in DOI collection.'.format(matad.doicoll.count())
        dois = matad.get_all_dois()
        print '{}/{} materials have DOIs.'.format(len(dois), matad.matcoll.count())
    elif args.plotly:
        import os, datetime
        import plotly.plotly as py
        from plotly.graph_objs import *
        stream_ids = ['645h22ynck', '96howh4ip8', 'nnqpv5ra02']
        py.sign_in(
            os.environ.get('MP_PLOTLY_USER'),
            os.environ.get('MP_PLOTLY_APIKEY'),
            stream_ids=stream_ids
        )
        today = datetime.date.today()
        counts = [
            matad.matcoll.count(), matad.doicoll.count(),
            len(matad.get_all_dois())
        ]
        names = ['materials', 'requested DOIs', 'validated DOIs']
        data = Data([
            Scatter(
                x=[today], y=[counts[idx]], name=names[idx],
                stream=dict(token=stream_ids[idx], maxpoints=10000)
            ) for idx,count in enumerate(counts)
        ])
        filename = 'dois_{}'.format(today)
        print py.plot(data, filename=filename, auto_open=False)
else:
    # generate records for either n or all (n=0) not-yet-submitted materials 
    # OR generate records for specific materials (submitted or not)
    osti = OstiRecord(l=args.l, n=args.n, db_yaml=db_yaml)
    osti.submit()
