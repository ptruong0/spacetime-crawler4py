from math import fabs
import re
from urllib.parse import urlparse, urldefrag, parse_qsl
import urllib.robotparser
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

def scraper(url, resp, robots=False):
    links, text = extract_next_links(url, resp, robots)
    return ([link for link in links if is_valid(link)], text)

def mostlySimilar(a, b):
    aParsed = urlparse(a)
    bParsed = urlparse(b)

    # if all matching except query
    return (aParsed.scheme == bParsed.scheme and aParsed.netloc == bParsed.netloc and aParsed.path == bParsed.path) or \
        (SequenceMatcher(None, a, b).ratio() > 0.95)
        
        
def tokenFrequencies(text: str, freq: dict) -> dict:
    if len(text) == 0: 
        return 0

    tokens = word_tokenize(text)
    alpha = re.compile(r'^[a-zA-Z]+$')
    nonAlphanumeric = re.compile(r'\W')
    numWords = 0

    # count how many appearances of each token in the token list
    for t in tokens:
        if len(t) > 1 and re.match(alpha, t):
            freq[t.lower()] += 1
            numWords += 1

    return numWords

def extract_next_links(url, resp, hasRobots):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!

    pageText = ''
    scrapedURLs = []
    if resp.status == 200:
        parentURL = urlparse(resp.raw_response.url)

        # create robots file parser if the site has a robots.txt file
        rfp = None
        if hasRobots:
            print('HAS ROBOTS.TXT')
            rfp = urllib.robotparser.RobotFileParser()
            rfp.set_url(parentURL.scheme + '://' + parentURL.netloc + '/robots.txt')
            rfp.read()
            if not rfp.can_fetch('*', resp.raw_response.url):
                return ([], '')

        # parse page content from response
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
        
        # tokenize page content
        pageText = soup.get_text().lower()

        # don't parse extremely large files 
        if len(pageText) > 10000:
            return([], '')

        # find every link on the page
        for link in soup.find_all('a'):
            pageURL = link.get('href')
            # invalid URL's
            if pageURL == None or len(pageURL) < 5 or type(pageURL) != str:
                continue

            urlParts = urlparse(pageURL)

            # remove fragment
            pageURL = urldefrag(pageURL).url

            # skip duplicate URLs (leads to the same page)
            if pageURL == urldefrag(resp.raw_response.url).url:
                continue

            # crawling not allowed in robots.txt
            if rfp != None and not rfp.can_fetch('*', pageURL):
                print('NO PERMISSIONS')
                continue

            # for relative paths, convert back to absolute paths
            if urlParts.scheme not in set(["http", "https"]) and urlParts.netloc == '':
                if pageURL.startswith('//'):
                    pageURL = parentURL.scheme + ':' + pageURL
                else:
                    pageURL = parentURL.scheme + '://' + parentURL.netloc + pageURL

            # add url to list
            scrapedURLs.append(pageURL)
    else:
        return ([], '')

    # prioritize links from a different domain
    # secondarily, prioritize short links (tend to give more info)
    scrapedURLs.sort(key=(lambda u: (urlparse(u).netloc == parentURL.netloc, len(u))))

    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    return (scrapedURLs, pageText)

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # check if in subdomain 
        if not (parsed.netloc.endswith('.ics.uci.edu')) and \
            not (parsed.netloc.endswith('.cs.uci.edu')) and \
                not (parsed.netloc.endswith('.informatics.uci.edu')) and \
                    not (parsed.netloc.endswith('.stat.uci.edu')) and \
                        not (parsed.netloc == 'today.uci.edu' and parsed.path.startswith('/department/information_computer_sciences/')):
            return False

        # check if not a calendar file
        if 'ical' in parsed.query:
            return False

        # check if too many query strings (tend to give little info)
        if len(parse_qsl(parsed.query)) > 8:
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|ics|ical|ifb"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
