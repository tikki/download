# encoding: utf-8

'''use download(url, path = None, overwrite_existing = False, timeout = None, retry = 0) to download a file

url is the url from where to download the data from
path is the local path where to save the data to
    if path is None, the data will be kept in memory and returned upon completion
overwrite_existing will overwrite existing files at path if set to True, otherwise it will raise an IOError
timeout is the time in seconds before a connection attempt will be aborted
    if timeout is None, a global default timeout setting will be used
retry is the amount times a connection attempt will be made after a connection failure by time out
    if retry is -1, connection attempts will be made until it succeeds
'''

import os, urllib, urllib2
from collections import OrderedDict as odict # used by forge headers
import cStringIO, gzip # used by gunzip
import time, email.utils # used to handle 'last-modified' header
import httplib
import tempfile
import shutil

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

def _gunzip_string(s):
    fo = cStringIO.StringIO(s)
    with gzip.GzipFile(fileobj=fo) as gz:
        return gz.read()

def _gunzip_file(path, in_place = True):
    # create a temporary file to hold the unzipped data
    tempfd, tempfile_path = tempfile.mkstemp(dir = '.')
    # unzip to temp file
    with gzip.open(path) as gz:
        try:
            while 1:
                new_data = gz.read(1500)
                if not new_data:
                    break
                os.write(tempfd, new_data)
        finally:
            os.close(tempfd)
    # replace file with temp file
    if not in_place:
        return tempfile_path
    else:
        try:
            os.rename(tempfile_path, path)
        except OSError: # "On Windows, if dst already exists, OSError will be raised"
            os.unlink(path)
            os.rename(tempfile_path, path)
        return True
    return False

def download(url, path = None, overwrite_existing = False, timeout = None, retry = 0):
    if not overwrite_existing and path is not None and os.path.exists(path) and os.path.getsize(path):
        raise IOError('file exists')
    headers = _forge_firefox_simple_headers(url)
    request = urllib2.Request(url, headers=headers)
    tried = 0
    while tried <= retry or retry == -1:
        is_last_try = tried == retry
        tried += 1
        try:
            site = urllib2.urlopen(request, timeout = timeout)
            break
        except httplib.BadStatusLine, e:
            if is_last_try:
                raise
        except urllib2.URLError, e:
            if len(e.args) != 1 or e.args[0].message != 'timed out' or is_last_try:
                raise
    is_gzipped = 'content-encoding' in site.headers.dict and site.headers.dict['content-encoding'] == 'gzip'
    if path is None:
        # read content
        try:
            content = site.read()
        finally:
            site.close()
        # unzip if necessary
        if is_gzipped:
            content = _gunzip_string(content)
        # return
        return content
    else:
        # write site content to temp file
        tempfd, tempfile_path = tempfile.mkstemp(dir = '.')
        try:
            while 1:
                new_content = site.read(1500)
                if not new_content:
                    break
                os.write(tempfd, new_content)
        finally:
            os.close(tempfd)
        # unzip if necessary
        if is_gzipped:
            if not _gunzip_file(tempfile_path, in_place = True):
                raise IOError('could not unzip `%s`' % tempfile_path)
        # create intermediate directories if necessary
        if os.path.dirname(path) and not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        # move to destination
        shutil.move(tempfile_path, path)
        # fix file attributes
        if 'last-modified' in site.headers.dict:
            last_modified = site.headers.dict['last-modified']
            mtime = email.utils.parsedate(last_modified)
            if mtime:
                mtime = time.mktime(mtime)
                os.utime(path, (mtime, mtime))
