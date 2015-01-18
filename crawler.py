#!/usr/bin/env python

"""Web Crawler/Spider

This modules implements a crawler to scrape http://www.appfun.cn
Though it is generic enough to work with any other url. But the extraction
will only work for the above metioned url which basically extracts apk(
Android Application Package) info.

@author: Mayur Swami
@date: 17/01/2015
@email: swamimayur0@gmail.com

"""

import re
import sys
import os
import time
import csv
import urllib2
import urlparse
import optparse
# from traceback import format_exc
from Queue import Queue
from datetime import datetime

from bs4 import BeautifulSoup


USAGE = "%prog [options] <url>"
VERSION = "%prog v" + '2.7'

AGENT = "%s/%s" % (__name__, '2.7')

APP_STORE = "app_results.csv"


def save_to_csv(app_data, field_order, app_store):
    """
    This method is required to save data in a csv file.
    :param: app_data: Map to field name and field value , specifically
                    field name is the extracted filed for an app.
    :type app_data: dict
    :param field_order: Order in which field will be displayed in csv.
    :type field_order: list
    """
    with_header = False
    if not os.path.exists(app_store):
        with_header = True
    with open(app_store, "a") as file_obj:
        # Make sure to convert unicode value to a utf-8 since chinese content
        # cant be directly stored in csv.
        app_data = {key:
                    value.encode('utf-8') if isinstance(
                        value, unicode) else value
                    for key, value in app_data.items()}
        app_writer = csv.DictWriter(file_obj, field_order)
        # Write header if app store is created freshly.
        if with_header:
            app_writer.writeheader()
        app_writer.writerow(app_data)


class Crawler(object):
    """
    A crawler to go crawl every possible link with the limit on depth.
    """
    def __init__(self, root, depth_limit, confine=None,
                 exclude=[], locked=True):
        self.root = root
        self.host = urlparse.urlparse(root)[1]

        # Data for filters:
        # Specify the maximum limit on crawl.
        self.depth_limit = depth_limit
        # Limit search to a single host.
        self.locked = locked
        # Limit search to this prefix.
        self.confine_prefix = confine
        # URL prefixes NOT to visit.
        self.exclude_prefixes = exclude
        # Container to hold  all available app links on a page.
        self.app_links = []
        # Visited links
        self.visited_links = []

        # Pre-visit filters:  Only visit a URL if it passes these tests
        self.pre_visit_filters = [self._prefix_ok, self._exclude_ok,
                                  self._not_visited, self._same_host]

    def _pre_visit_url_condense(self, url):
        """ Reduce (condense) URLs into some canonical form before
        visiting.  All occurrences of equivalent URLs are treated as
        identical.

        All this does is strip the \"fragment\" component from URLs,
        so that http://foo.com/blah.html\#baz becomes
        http://foo.com/blah.html """

        base, _ = urlparse.urldefrag(url)
        return base

    # URL Filtering functions. These all use information from the
    # state of the Crawler to evaluate whether a given URL should be
    # used in some context.  Return value of True indicates that the
    # URL should be used.
    def _prefix_ok(self, url):
        """Pass if the URL has the correct prefix, or none is specified"""
        return (self.confine_prefix is None or
                url.startswith(self.confine_prefix))

    def _exclude_ok(self, url):
        """Pass if the URL does not match any exclude patterns"""
        prefixes_ok = [not url.startswith(p) for p in self.exclude_prefixes]
        return all(prefixes_ok)

    def _not_visited(self, url):
        """Pass if the URL has not already been visited"""
        return url not in self.visited_links

    def _same_host(self, url):
        """Pass if the URL is on the same host as the root URL"""
        try:
            host = urlparse.urlparse(url)[1]
            return re.match(".*%s" % self.host, host)
        except Exception, e:
            print >> sys.stderr, "ERROR: Can't process url '%s' (%s)" % (
                url, e)
            return False

    def _add_host(self, url):
        """ Add host to a url. """
        return "http://" + self.host + url

    def crawl(self, store=APP_STORE):

        """ Main function in the crawling process.  Core algorithm is:

        q <- starting page
        while q not empty:
           url <- q.get()
           if url is new and suitable:
              page <- fetch(url)
              q.put(urls found in page)
           else:
              nothing

        new and suitable means that we don't re-visit URLs we've seen
        already fetched, and user-supplied criteria like maximum
        search depth are checked. """
        q = Queue()
        # Crawl till the depth specified.
        for depth in xrange(1, 1299):
            if depth <= self.depth_limit:
                ml = self.root.replace("1", str(depth))
                q.put((ml, depth))
            else:
                break
        print "Total pages to crawl: {}".format(q.qsize())
        while not q.empty():
            this_url, depth = q.get()

            # Apply URL-based filters.
            do_not_follow = [f for f in self.pre_visit_filters
                             if not f(this_url)]

            # Special-case depth 0 (starting URL)
            if [] != do_not_follow:
                print >> sys.stderr, "Whoops! Starting URL %s rejected by the \
                following filters:", do_not_follow

            # If no filters failed (that is, all passed), process URL
            if [] == do_not_follow:
                try:
                    self.visited_links.append(this_url)
                    print "Depth {} Fetch {}".format(depth, this_url)
                    page_result = Fetcher(this_url, store).fetch()

                    if page_result[1]:
                        self.app_links.extend(page_result[0])
                    else:
                        continue
                except Exception, e:
                    print >>sys.stderr, "ERROR: Can't process url '%s' (%s) \
                    " % (this_url, e)
                    # print format_exc()
        # Once app links from every page is collected then start extracting
        # app.
        # Sanitize all app links before extracting app.
        print "Extracting app info"
        for app_link in [self._pre_visit_url_condense(l)
                         for l in self.app_links][:5]:
            app_link = self._add_host(app_link)
            print "Validate and extract {}".format(app_link)
            if self._not_visited(app_link):
                self.visited_links.append(app_link)
                app_fetch = Fetcher(app_link, store)
                app_fetch.fetch(with_app_meta=True)


class DataException(Exception):
    def __init__(self, message, mimetype, url):
        super(DataException, self).__init__(message)
        self.mimetype = mimetype
        self.url = url


class Fetcher(object):
    """
    Serves fetching and extraction of data.
    """
    def __init__(self, url, store):
        self.url = url
        self.store = store

    def _addHeaders(self, request):
        request.add_header("User-Agent", AGENT)

    def _open(self):
        url = self.url
        try:
            request = urllib2.Request(url)
            handle = urllib2.build_opener()
        except IOError:
            return None
        return (request, handle)

    def fetch(self, with_app_meta=False):
        """
        Main method to fetch and extract data . If flag `with_app_meta` is set
        then extraction of app is processed  otherwise app links will be
        collected.
        """
        request, handle = self._open()
        self._addHeaders(request)
        status = False
        soup = None
        if handle:
            try:
                data = handle.open(request)
                mime_type = data.info().gettype()
                url = data.geturl()
                if mime_type != "text/html":
                    raise DataException(
                        "Not interested in files of type %s \
                        " % mime_type, mime_type, url)
                content = unicode(
                    data.read(), "utf-8", errors="replace")
                soup = BeautifulSoup(content)
                status = True
                if not soup:
                    return []
            except urllib2.HTTPError, error:
                if error.code == 404:
                    print >> sys.stderr, "ERROR: %s -> %s" % (error, error.url)
                else:
                    print >> sys.stderr, "ERROR: %s" % error
            except urllib2.URLError, error:
                print >> sys.stderr, "ERROR: %s" % error
            except DataException, error:
                print >>sys.stderr, "Skipping %s, has type %s \
                        " % (error.url, error.mimetype)
            if not with_app_meta:
                app_links = [dtag.a.get('href') for dtag in soup.findAll(
                    'div', attrs={'class': 'app-icon'})]
                return app_links, status
            else:
                self._extract_app_info(soup)

    def _extract_app_info(self, soup):
        """
        Extract all possible information from an app.
        """
        title = soup.find(
            'div', attrs={'class': 'content-categoryCtn-title'}).h1.text
        # List containg detailed info about app viz. Category, Version,
        # Size, Last Updated, Developer
        app_data = {}
        try:
            app_info = [(
                l.span.text, l.div.text) for l in soup.find(
                    'ul', attrs={'class': 'sideBar-appDetail'}).findAll('li')]
            download_link = soup.find(
                'div', attrs={'class': 'content-detailCtn-icon'}).a.get('href')
            image_link = soup.find(
                'div', attrs={'class': 'content-detailCtn-icon'}).p.img.get(
                    'src')
            app_intro = soup.find(
                'div', attrs={'class': 'content-detailCtn-text'}).select(
                    'div > div')[0].text
            app_pics = [dv.img.get('src')
                        for dv in soup.find(
                            'div', attrs={'class': 'slide-content'}).select(
                                'div > div')]
        except AttributeError:
            return
        # prepare app data to be saved in csv file
        app_data['Title'] = title
        app_data['Category'] = app_info[0][1]
        app_data['Version'] = app_info[1][1]
        app_data['Size'] = app_info[2][1]
        app_data['Last Updated'] = app_info[3][1]
        app_data['Developer'] = app_info[4][1]
        app_data['Download_link'] = download_link
        app_data['Image_link'] = image_link
        app_data['Introduction'] = app_intro
        app_data['Other_pics_link'] = ",".join(app_pics)
        app_data['Crawl Time'] = str(datetime.now())
        app_data['App Link'] = self.url
        field_order = ['Title', 'Introduction', 'Category', 'Version',
                       'Size', 'Last Updated', 'App Link', 'Download_link',
                       'Image_link', 'Other_pics_link', 'Developer',
                       'Crawl Time']

        # Save extracted info into a csv file.
        save_to_csv(app_data, field_order, self.store)


def parse_options():
    """parse_options() -> opts, args

    Parse any command-line options given returning both
    the parsed options and arguments.
    """

    parser = optparse.OptionParser(usage=USAGE, version=VERSION)

    parser.add_option("-q", "--quiet", action="store_true", default=False,
                      dest="quiet", help="Enable quiet mode")

    parser.add_option("-d", "--depth", action="store", type="int",
                      default=30, dest="depth_limit",
                      help="Maximum depth to traverse")

    parser.add_option("-c", "--confine", action="store", type="string",
                      dest="confine",
                      help="Confine crawl to specified prefix")

    parser.add_option("-x", "--exclude", action="append", type="string",
                      dest="exclude", default=[],
                      help="Exclude URLs by prefix")

    opts, args = parser.parse_args()
    '''
    if len(args) < 1:
        parser.print_help(sys.stderr)
        raise SystemExit, 1
    '''
    return opts, args


def main():
    opts, args = parse_options()
    try:
        url = args[0]
    except IndexError:
        url = "http://www.appfun.cn/soft/applist/cid/9/page/1"

    depth_limit = opts.depth_limit
    confine_prefix = opts.confine
    exclude = opts.exclude

    start_time = time.time()

    print >> sys.stderr, "Crawling %s (Max Depth: %d)" % (url, depth_limit)
    crawler = Crawler(url, depth_limit, confine_prefix, exclude)
    crawler.crawl()

    end_time = time.time()
    total_time = end_time - start_time
    print >> sys.stderr, "Crawling finished in {} seconds".format(total_time)

if __name__ == "__main__":
    main()
