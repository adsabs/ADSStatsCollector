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
    parser.add_argument('--no-push',
                        dest='no_push',
                        default=False,
                        action='store_true',
                        help='Compute stats but do not push them to prometheus pushgateway')
    parser.add_argument('--graylog',
                        dest='graylog',
                        default=False,
                        action='store_true',
                        help='Compute stats from graylog')
    parser.add_argument('--solr',
                        dest='solr',
                        default=False,
                        action='store_true',
                        help='Compute stats from solr')
    parser.add_argument('--postgres',
                        dest='postgres',
                        default=False,
                        action='store_true',
                        help='Compute stats from postgres')
    parser.add_argument('--classic',
                        dest='classic',
                        default=False,
                        action='store_true',
                        help='Compare bibcodes registered in postgres and solr against classic')
    parser.add_argument('--no-classic-upload',
                        dest='no_classic_upload',
                        default=False,
                        action='store_true',
                        help='Do not upload files with missing/extra bibcodes to Google Team Drive')
    args = parser.parse_args()

    if args.verify_access:
        googledrive.verify_access()
        sys.exit(0)
    else:
        if args.graylog:
            # ~1 second
            logs_stats = graylog.stats()
            prometheus.push("logs", logs_stats, simulate=args.no_push)

        if args.solr:
            # ~1 second
            solr_stats = solr.stats()
            prometheus.push("solr", solr_stats, simulate=args.no_push)

        if args.postgres:
            # ~5 minutes
            db_stats = postgres.stats()
            prometheus.push("master_pipeline_records", db_stats, simulate=args.no_push)

        if args.classic:
            # ~15 minutes
            classic_bibcodes = classic.bibcodes()
            db_bibcodes = postgres.bibcodes()
            solr_bibcodes = solr.bibcodes()
            bibcodes_stats, bibcodes_batch = classic.compare(classic_bibcodes, db_bibcodes, solr_bibcodes)
            prometheus.push("classic", bibcodes_stats, simulate=args.no_push)
            if not args.no_classic_upload:
                googledrive.upload(bibcodes_batch, keep_last_n_folders=config.get('GOOGLE_DRIVE_KEEP_LAST_N_FOLDERS', 30))

