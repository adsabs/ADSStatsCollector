import os
import requests
import argparse
from adsputils import setup_logging, load_config
from statscollector import postgres
from statscollector import solr
from statscollector import prometheus
from statscollector import graylog
from statscollector import classic
from statscollector import googledrive
from statscollector.setup import config, logger



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collect statistics')

    parser.add_argument('--verify-access',
                        dest='verify_access',
                        default=False,
                        action='store_true',
                        help='Verify Google Team Drive access')
    parser.add_argument('-s', '--simulate',
                        dest='simulate',
                        default=False,
                        action='store_true',
                        help='Compute stats but do not push them to prometheus')
    parser.add_argument('--compare-bibcodes',
                        dest='compare_bibcodes',
                        default=False,
                        action='store_true',
                        help='Compare bibcodes registered in classic, postgres and solr (longer execution time, ~15 minutes)')
    args = parser.parse_args()

    if args.verify_access:
        googledrive.verify_access()
    else:
        if args.compare_bibcodes:
            # ~15 minutes
            classic_bibcodes = classic.bibcodes()
            db_bibcodes = postgres.bibcodes()
            solr_bibcodes = solr.bibcodes()
            bibcodes_stats, bibcodes_batch = classic.compare(classic_bibcodes, db_bibcodes, solr_bibcodes)
            prometheus.push("classic", bibcodes_stats, simulate=args.simulate)
            if not args.simulate:
                googledrive.upload(bibcodes_batch)
        else:
            # ~1 second
            logs_stats = graylog.stats()
            prometheus.push("logs", logs_stats, simulate=args.simulate)

            # ~1 second
            solr_stats = solr.stats()
            prometheus.push("solr", solr_stats, simulate=args.simulate)

            # ~5 minutes
            db_stats = postgres.stats()
            prometheus.push("master_pipeline_records", db_stats, simulate=args.simulate)
