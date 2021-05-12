import argparse
import json
import os
import psycopg2
from glob import glob
from datetime import datetime

# ============================= INITIALIZATION ==================================== #
from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
config = load_config(proj_home=proj_home)
logger = setup_logging('fulltext', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))


def build_input_list():
    """
    Assembles a list of bibstems that we have fulltext sources for by checking in the directories where
    publisher-provided XML files and input PDFs live
    :return: list of bibstems for which we have input fulltext source files
    """

    # in these directories, there are subdirectories named per bibstem. There are also other subdirectories there that
    # aren't named for bibstems, but the bibstem subdirectories all start with a capital letter, so just get those
    xml_bibstems = [os.path.basename(os.path.normpath(x)) for x in glob(os.path.join(config.get('SOURCES_DIR'), '[A-Z]*', ''))]
    pdf_bibstems = [os.path.basename(os.path.normpath(x)) for x in glob(os.path.join(config.get('PDF_SOURCES_DIR'), '[A-Z]*', ''))]

    sources = list(set(xml_bibstems + pdf_bibstems))

    return sources


def bibcode_monitoring(year, bibstems=None):
    """
    Monitoring script to check for outlier bibcodes compared to others from the same year + bibstem. To be
    run regularly (weekly or monthly) - logs bibcodes that have no fulltext body extracted (though
    we'd expect bibcodes with the given bibstem to have fulltext) or those with an unusually short extracted body
    compared to other bibcodes from the given year + bibstem
    :param year: year to run the script for
    :param bibstems: list of bibstems to run the script for
    :return: none (logs output only)
    """
    if not bibstems:
        # get the list of input bibstems
        bibstems = build_input_list()

    master_connection = None
    try:
        master_connection = psycopg2.connect(host=host,
                                             port=port,
                                             database=master_pipeline_db,
                                             user=user,
                                             password=password)

        with master_connection.cursor() as master_cursor:
            for bibstem in bibstems:
                logger.debug('Checking bibcodes from year %s from bibstem %s', year, bibstem)
                bibstem_year = str(year) + bibstem
                fulltext_no_null = "(regexp_replace(fulltext::text, '\\\\u0000', '', 'g'))"
                # for each bibstem, check if any have the fulltext field but are missing the body
                no_body_query = """
                SELECT bibcode FROM records 
                WHERE bibcode LIKE '{0}%' AND fulltext IS NOT NULL AND NOT ({1}::jsonb ? 'body');
                """.format(bibstem_year, fulltext_no_null)
                master_cursor.execute(no_body_query)
                for n in master_cursor.fetchall():
                    logger.info('Bibcode %s has extracted fulltext but no body was extracted.', n[0])

                # for each bibstem, check the length of the extracted body against the average
                stats_query = """
                SELECT avg(length(fulltext)), 
                       stddev_samp(length(fulltext)) 
                FROM records 
                WHERE bibcode LIKE '{0}%' AND ({1}::jsonb ? 'body');
                """.format(bibstem_year, fulltext_no_null)
                master_cursor.execute(stats_query)
                avg, stddev = master_cursor.fetchone()
                if not avg or not stddev:
                    logger.debug('Bibstem %s has no (or only one) extracted fulltext body for year %s.', bibstem, year)
                    continue
                avg = float(avg)
                stddev = float(stddev)

                body_query = """
                SELECT bibcode,  length({1}::jsonb->>'body') FROM records 
                WHERE bibcode LIKE '{0}%' AND ({1}::jsonb ? 'body');
                """.format(bibstem_year, fulltext_no_null)
                master_cursor.execute(body_query)

                for bf in master_cursor.fetchall():
                    if bf[1] < avg - (config.get('STDDEV_CUTOFF', 1.5) * stddev):
                        logger.info('Bibcode %s has extracted fulltext but body (length: %s) is short compared '
                                    'to similar bibcodes (avg: %s, stddev: %s)', bf[0], bf[1], avg, stddev)
    except:
        logger.exception("Failed retrieving stats from postgres")
    finally:
        if master_connection is not None:
            master_connection.close()

def bibstem_monitoring(bibstems=None):
    """
    Monitoring script to compare average fulltext output for a given year + bibstem to previous years for the same
    bibstem to check for outlier years. To be run on an ad hoc basis. Logs years + bibstems that have an unusually
    small number of bibcodes with extracted body text, or unusually short body text fields, compared to the prior year
    :param bibstems: List of bibstems to run the script for
    :return: none (logs output only)
    """
    if not bibstems:
        # get the list of input bibstems
        bibstems = build_input_list()

    master_connection = None
    try:
        master_connection = psycopg2.connect(host=host,
                                             port=port,
                                             database=master_pipeline_db,
                                             user=user,
                                             password=password)

        with master_connection.cursor() as master_cursor:
            for bibstem in bibstems:
                # for each bibstem, get the average stats on fulltext by year, from 2000 onwards, to compare year-by-year
                # _ is the single character wildcard for postgres
                bibstem_year = '20__' + bibstem
                stats_query = """
                SELECT avg(length(fulltext)), 
                       stddev_samp(length(fulltext)), 
                       left(bibcode,4) AS year, 
                       count(*) AS num FROM records 
                WHERE bibcode LIKE '{0}%' AND (fulltext::jsonb ? 'body') GROUP BY left(bibcode,4);
                """.format(bibstem_year)

                master_cursor.execute(stats_query)
                avg = []
                stddev = []
                years = []
                num = []
                for m in master_cursor:
                    avg.append(float(m[0]))
                    stddev.append(float(m[1]))
                    years.append(m[2])
                    num.append(m[3])

                noise = [i**0.5 for i in num]
                for idx, n in enumerate(noise[:-1]):
                    # check if too few records have an extracted body, compared to prior year
                    if (num[idx] - (config.get('COUNT_ERR', 5) * n)) > num[idx+1]:
                        logger.info('For bibstem %s, year %s has an anomalously low fulltext body count. Count: %s (prior year count: %s)', bibstem, years[idx+1], num[idx+1], num[idx])
                    # check if the average length of the extracted body is too short, compared to prior year
                    if (avg[idx] - (config.get('STDDEV_CUTOFF', 1.5) * stddev[idx])) > avg[idx+1]:
                        logger.info('For bibstem %s, year %s has an anomalously low average body length. Avg: %s (prior year average: %s)', bibstem, years[idx+1], avg[idx+1], avg[idx])
    except:
        logger.exception("Failed retrieving stats from postgres")
    finally:
        if master_connection is not None:
            master_connection.close()


if __name__ == '__main__':
    # Runs reporting scripts, outputs results to logs

    host = config.get('POSTGRES_HOST')
    port = config.get('POSTGRES_PORT')
    user = config.get('POSTGRES_USER')
    password = config.get('POSTGRES_PASSWORD')
    master_pipeline_db = config.get('POSTGRES_MASTER_PIPELINE_DB')

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-y',
                        '--year',
                        dest='year',
                        action='store',
                        default=None,
                        help='For monitoring script, year to run script for, if not current year')

    parser.add_argument('-b',
                        '--bibstem',
                        dest='bibstem',
                        action='store',
                        default=None,
                        help='For monitoring script, comma-separated list bibstem(s) to run script for, if not all bibstems')

    parser.add_argument('-hi',
                        '--historical',
                        dest='historical',
                        action='store_true',
                        default=False,
                        help='For monitoring script, flag to run script for year-by-year historical comparisons')

    args = parser.parse_args()

    if args.bibstem:
        args.bibstem = [x.strip() for x in args.bibstem.split(',')]

    if args.historical:
        logger.info(f"Running historical monitoring for {', '.join(args.bibstem) if args.bibstem else 'all'} bibstems.")
        bibstem_monitoring(args.bibstem)

    else:
        if args.year:
            logger.info(f"Running monitoring for {', '.join(args.bibstem) if args.bibstem else 'all'} bibstems for year {args.year}.")
            bibcode_monitoring(args.year, args.bibstem)
        else:
            today = datetime.today()
            logger.info(f"Running monitoring for {', '.join(args.bibstem) if args.bibstem else 'all'} bibstems for current year.")
            bibcode_monitoring(today.year, args.bibstem)
