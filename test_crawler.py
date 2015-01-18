"""
This module provides unit test cases for crawler.py.
"""
import os
import unittest
import csv
import urllib2

from bs4 import BeautifulSoup

from crawler import Crawler, Fetcher


class TestCrawler(unittest.TestCase):
    def setUp(self):
        self.root = 'http://www.appfun.cn/soft/applist/cid/9/page/1'
        self.depth = 1
        self.app_link = 'http://www.appfun.cn/app/info/appid/80146'
        self.test_app_store = "test_app_store.csv"

    def test_crawl(self):
        # Instantiate crawler and get all app_links
        crawler = Crawler(self.root, self.depth)
        crawler.crawl(self.test_app_store)
        self.assertGreater(len(crawler.app_links), 1)
        self.assertGreater(len(crawler.visited_links), 1)

        # check if we get full app url.
        try:
            app_link = crawler.app_links[0]
            app_link = crawler._add_host(app_link)
            self.assertTrue(app_link.startswith("http://www.appfun.cn"))
        except IndexError:
            pass

        # Now test app link and main page individually.
        page_result = Fetcher(self.root, self.test_app_store).fetch()
        if page_result[1]:
            self.assertGreaterEqual(page_result[0], 40)

        # After extracting app info the cound of csv file must increase by 1.
        # It requires to iterated through every line and hold the count.
        with open(self.test_app_store, "rb") as fobj:
            reader = csv.reader(fobj)
            count_before_add = sum(1 for r in reader)

        Fetcher(self.app_link, self.test_app_store).fetch(with_app_meta=True)

        with open(self.test_app_store, "rb") as fobj:
            reader = csv.reader(fobj)
            count_after_add = sum(1 for r in reader)
        self.assertEqual(count_before_add + 1, count_after_add)

    def test_extract_app_info(self):
        page_response = urllib2.urlopen(self.app_link)
        page_content = unicode(page_response.read(), 'utf-8', errors='replace')
        soup = BeautifulSoup(page_content)
        link_fetch = Fetcher(self.app_link, self.test_app_store)
        link_fetch._extract_app_info(soup)
        # Verify that title and download link are same in csv file and the
        # fetched content.
        title = soup.find(
            'div', attrs={'class': 'content-categoryCtn-title'}).h1.text
        title = title.encode('utf-8')
        download_link = soup.find(
            'div', attrs={'class': 'content-detailCtn-icon'}).a.get('href')

        with open(self.test_app_store, "r") as fobj:
            app_reader = csv.DictReader(fobj)
            app_info = app_reader.next()
            self.assertEqual(title, app_info.get('Title'))
            self.assertEqual(download_link, app_info.get('Download_link'))


    def tearDown(self):
        os.remove(self.test_app_store)

if __name__ == "__main__":
    unittest.main()

