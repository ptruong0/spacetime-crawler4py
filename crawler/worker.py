from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
from collections import defaultdict
from urllib.parse import urlparse
import requests

import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, reporter):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.reporter = reporter

        self.similar_count = 0      # counts number of subsequent URLs in the frontier
        self.prev_url = ''          # keep track of previous crawled URL to detect similarities
        self.bad_url = True         # keep track of URLs which give a non-200 status code

        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests from scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            parsed = urlparse(tbd_url)
            print('Scraping', tbd_url)

            # this subdomain caused many problems - error codes, takes too long, no useful page content
            if parsed.netloc == 'swiki.ics.uci.edu':
                self.reporter.addPage(tbd_url)
                continue
            
            # don't crawl too many similar URL's subsequently, to avoid loops
            if scraper.mostlySimilar(tbd_url, self.prev_url):
                print('similar:', self.similar_count)
                self.prev_url = tbd_url
                self.similar_count += 1
                if self.similar_count > 10:
                    self.reporter.addPage(tbd_url)
                    continue
                
                # avoid URL's that gave download errors on previous crawls
                if self.bad_url:
                    continue
            else:
                self.similar_count = 0      # different URL, reset count
            
            self.prev_url = tbd_url

            # download URL
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            if resp.status != 200:
                print(tbd_url, 'gave error code', resp.status)
                self.bad_url = True
                continue
            else:
                self.bad_url = False

            # check if site has a robots.txt file
            hasRobots = False
            robotsResp = requests.get(parsed.scheme + '://' + parsed.netloc + '/robots.txt')
            if robotsResp.status_code == 200:
                hasRobots = True

            # scrape URLs from webpage, also get page contents
            scraped_urls, page_text = scraper.scraper(tbd_url, resp, robots=hasRobots)
            self.reporter.collect_data(tbd_url, page_text)

            # add scraped URLs to frontier
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
        
        # report statistics when finished
        self.reporter.writeReport()

