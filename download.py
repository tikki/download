# encoding: utf-8

'''use download(url, path = None, overwrite_existing = False) to download a file
'''

import os, urllib, urllib2
from collections import OrderedDict as odict # used by forge headers
import cStringIO, gzip # used by gunzip

def _forge_firefox_simple_headers(url): # referer = None
    headers = odict()
    headers['Host'] = urllib.splithost(url[url.find('://')+1:])[0]
    headers['User-Agent'] = r'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b11) Gecko/20100101 Firefox/4.0b11'
    headers['Accept'] = r'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    headers['Accept-Language'] = r'en-us,en;q=0.5'
    headers['Accept-Encoding'] = r'gzip, deflate'
    headers['Accept-Charset'] = r'ISO-8859-1,utf-8;q=0.7,*;q=0.7'
    headers['Keep-Alive'] = r'115'
    headers['Connection'] = r'keep-alive'
    return headers

def _gunzip(s):
    fo = cStringIO.StringIO(s)
    with gzip.GzipFile(fileobj=fo) as gz:
        return gz.read()

def download(url, path = None, overwrite_existing = False):
    if not overwrite_existing and path is not None and os.path.exists(path) and os.path.getsize(path):
        raise IOError('file exists')
    headers = _forge_firefox_simple_headers(url)
    request = urllib2.Request(url, headers=headers)
    site = urllib2.urlopen(request)
    try:
        content = site.read()
        if 'content-encoding' in site.headers.dict and site.headers.dict['content-encoding'] == 'gzip':
            content = _gunzip(content)
    finally:
        site.close()
    if path is None:
        return content
    else:
        if os.path.dirname(path) and not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        with open(path, 'wb') as fd:
            fd.write(content)

