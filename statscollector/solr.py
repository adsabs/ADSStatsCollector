import requests
from urllib.parse import urljoin
from .setup import config, logger

def _updates(solr_url):
    results = {}
    query = 'admin/mbeans?stats=true&cat=UPDATE&wt=json'
    r = requests.get(urljoin(solr_url, query), timeout=30)
    r.raise_for_status()
    j = r.json()
    updateHandler_stats = j.get('solr-mbeans', [{}, {}])[1].get('updateHandler', {}).get('stats', {})
    results['commits'] = updateHandler_stats.get('UPDATE.updateHandler.commits.count')
    results['cumulative_adds'] = updateHandler_stats.get('UPDATE.updateHandler.cumulativeAdds.count')
    results['cumulative_errors'] = updateHandler_stats.get('UPDATE.updateHandler.cumulativeErrors.count')
    results['errors'] = updateHandler_stats.get('UPDATE.updateHandler.errors')
    return results

def _index(solr_url):
    results = {}
    query = 'replication?command=details&wt=json'
    r = requests.get(urljoin(solr_url, query), timeout=30)
    r.raise_for_status()
    j = r.json()
    details = j.get('details', {})
    master = details.get('master', {})
    size, unit = details.get('indexSize', "0 GB").split(maxsplit=1)
    if unit == "TB":
        size = float(size) * 1024
    elif unit == "GB":
        size = float(size)
    elif unit == "MB":
        size = float(size) / 1024
    elif unit == "KB":
        size = float(size) / 1024 / 1024
    elif unit == "bytes":
        size = float(size) / 1024 / 1024 / 1024
    results['index_size'] = float("{:.2f}".format(size))
    results['version'] = master.get('replicableVersion')
    results['generation'] = master.get('replicableGeneration')
    return results

def _content(solr_url):
    results = {}
    query = 'select?q=*:*&rows=0&stats=true&stats.field=citation_count&stats.field=citation_count_norm'
    r = requests.get(urljoin(solr_url, query), timeout=30)
    r.raise_for_status()
    j = r.json()
    results['num_found'] = j.get('response', {}).get('numFound')
    stats_fields = j.get('stats', {}).get('stats_fields', {})
    results['citation_count'] = stats_fields.get('citation_count', {}).get('sum')
    if results['citation_count']:
        results['citation_count'] = int(results['citation_count'])
    results['citation_count_norm'] = stats_fields.get('citation_count_norm', {}).get('sum')
    if results['citation_count_norm']:
        results['citation_count_norm'] = float("{:.2f}".format(results['citation_count_norm']))
    return results

def stats():
    solr_url = config.get('SOLR_URL')

    results = {}
    try:
        results.update(_updates(solr_url))
    except:
        logger.exception("Failed retreiving update stats from solr")
    try:
        results.update(_index(solr_url))
    except:
        logger.exception("Failed retreiving index stats from solr")
    try:
        results.update(_content(solr_url))
    except:
        logger.exception("Failed retreiving content stats from solr")

    return results


def bibcodes():
    solr_url = config.get('SOLR_URL')
    query = 'select?fl=bibcode&cursorMark={}&q=*%3A*&rows=20000&sort=bibcode%20asc%2Cid%20asc&wt=json'
    bibcodes = []

    try:
        current_cursormark = '*'
        last_cursormark = None
        while current_cursormark != last_cursormark:
            url = urljoin(solr_url, query.format(current_cursormark))
            r = requests.get(url)
            r.raise_for_status()
            last_cursormark = current_cursormark
            j = r.json()
            current_cursormark = j.get('nextCursorMark')
            docs = j.get('response', {}).get('docs', [])
            bibcodes.extend(x['bibcode'] for x in docs)
    except:
        logger.exception("Failed retrieving bibcodes from solr")
        return []
    else:
        return bibcodes

