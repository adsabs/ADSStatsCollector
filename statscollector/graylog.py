from urllib.parse import urljoin
from datetime import datetime
from dateutil.parser import parse as tsparse
from dateutil.relativedelta import relativedelta as tsdelta
from grapi.grapi import Grapi
from .setup import config, logger


def _make_graylog_query (query, from_ts, until_ts, offset=0, limit=1000, fields='', sort='timestamp:asc'):
    """
    returns a dict that can be used to query Graylog for
    entries matching a particular query string in a given
    time interval
    """
    return {
        "query":  query,
        "from":   from_ts,
        "to":     until_ts,
        "offset": offset,
        "limit":  limit,
        "fields": fields,
        "sort":   sort
        }

def _myads_emails(api, start, end):
    results = {}
    fields = ",".join(["timestamp", "namespace_name", "container_name", "message"])
    query = config.get('GRAYLOG_MYADS_QUERY')
    r = api.send("get", **_make_graylog_query(query, start, end, limit=1, fields=fields))
    r.raise_for_status()
    j = r.json()
    results['myads_pipeline_emails'] = j.get('total_results')
    return results

def _container(api, start, end, container_name):
    results = {}
    fields = ",".join(["timestamp", "namespace_name", "container_name", "message"])
    query = config.get('GRAYLOG_CONTAINER_QUERY', '').format(container_name)
    r = api.send("get", **_make_graylog_query(query, start, end, limit=1, fields=fields))
    r.raise_for_status()
    j = r.json()
    results[container_name] = j.get('total_results')
    return results

def stats():
    url = urljoin(config.get('GRAYLOG_URL'), "api/search/universal/absolute")
    api = Grapi(url, config.get('GRAYLOG_TOKEN'))
    now = datetime.utcnow()
    before = now - tsdelta(hours=1)
    start = before.isoformat(sep=' ', timespec='milliseconds')
    end = (now + tsdelta(minutes=1)).isoformat(sep=' ', timespec='milliseconds')

    results = {}
    try:
        results.update(_myads_emails(api, start, end))
    except:
        logger.exception("Unable to retrieve myads emails logs from graylog")

    for container_name in config.get('GRAYLOG_CONTAINER_NAMES', []):
        try:
            results.update(_container(api, start, end, container_name))
        except:
            logger.exception("Unable to retrieve '%s' logs from graylog", container_name)

    return results
