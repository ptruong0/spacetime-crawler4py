from math import fabs
import re
from urllib.parse import urlparse, urldefrag, parse_qsl
import urllib.robotparser
from nltk.tokenize import word_tokenize
from collections import defaultdict
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

def scraper(url, resp, allFreq, robots=False):
    links, numWords = extract_next_links(url, resp, allFreq, robots)
    return ([link for link in links if is_valid(link)], numWords)

def mostlySimilar(a, b):
    ''' Returns true if (1) the two URL's share the same scheme, domain, and path, or (2) if they are 95% textually similar '''
    aParsed = urlparse(a)
    bParsed = urlparse(b)

    # if all matching except query or
    # if all 
    return (aParsed.scheme == bParsed.scheme and aParsed.netloc == bParsed.netloc and \
    (aParsed.path == bParsed.path or (aParsed.path != '' and bParsed.path != '' and aParsed.path.split('/')[:-1] == bParsed.path.split('/')[:-1]))) or \
        (SequenceMatcher(None, a, b).ratio() >= 0.95)
        
def containsDate(s):
    parts = s.split('-')
    if len(parts) < 3:
        return False
    middleIndex = -1
    for i in range(len(parts)):
        if len(parts[i]) == 2:
            middleIndex = i
            break
    if middleIndex == -1 or not parts[middleIndex].isdigit():
        return False
    if middleIndex + 1 < len(parts) and len(parts[middleIndex+1]) >= 2 and not parts[middleIndex+1][0:2].isdigit():
        return False
    if middleIndex - 1 >= 0 and len(parts[middleIndex-1]) >= 2 and not parts[middleIndex-1][-2:].isdigit():
        return False
    print('contains date')
    return True


        
def tokenFrequencies(text: str, freq: dict):
    ''' Counts occurrences of words in the text and records their frequencies. Returns number of words in the text. '''
    if len(text) == 0: 
        return 0
    tokens = word_tokenize(text)
    alpha = re.compile(r'^[a-zA-Z]+$')
    numWords = 0
    pageFreq = defaultdict(int)

    # count how many appearances of each token in the token list
    for t in tokens:
        if len(t) > 1 and re.match(alpha, t):
            freq[t.lower()] += 1
            pageFreq[t.lower()] += 1
            numWords += 1

    return (numWords, pageFreq)


def extract_next_links(url, resp, allFreq, hasRobots):
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
    numWords = 0
    try:
        if resp.status == 200:
            parentURL = urlparse(resp.raw_response.url)

            # create robots file parser if the site has a robots.txt file
            rfp = None
            if hasRobots:
                print('HAS ROBOTS.TXT', parentURL.scheme + '://' + parentURL.netloc + '/robots.txt')
                rfp = urllib.robotparser.RobotFileParser()
                rfp.set_url(parentURL.scheme + '://' + parentURL.netloc + '/robots.txt')
                rfp.read()
                if not rfp.can_fetch('*', resp.raw_response.url):
                    return ([], 0)

            # parse page content from response
            soup = BeautifulSoup(resp.raw_response.content, "lxml")
            
            # tokenize page content
            pageText = soup.get_text().lower()

            numWords, pageFreq = tokenFrequencies(pageText, allFreq)

            # skip pages with very few distinct words
            if len(pageFreq.keys()) < 10:
                return ([], 0)

            # skip pages that have spam words
            if len(pageFreq.keys()) > 0 and pageFreq[sorted(pageFreq.keys(), key=(lambda x: pageFreq[x]), reverse=True)[0]] / float(numWords) >= 0.5:
                return ([],  0)

            # don't parse extremely large files or extremely small files
            if numWords > 10000 or numWords < 50:
                return([], 0)

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

                # check if crawling is allowed in robots.txt
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
            return ([], 0)


        # prioritize links from a different domain
        # secondarily, prioritize short links (tend to give more info)
        scrapedURLs.sort(key=(lambda u: (urlparse(u).netloc == parentURL.netloc, len(u))))

    except:
        print('ERROR CAUGHT, SKIP PAGE')
        return ([], 0)

    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    return (scrapedURLs, numWords)

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

        # skip media files, social media redirects, search/filter results
        if any(suffix in parsed.query for suffix in ['ical', 'png', 'jpg', 'gif', 'pdf', 'facebook', 'zip','twitter', 'difftype', 'filter', 'odc']) or url.endswith('.Z'):
            return False

        # check if too many query strings (these pages tend to be too specific, give little info)
        if len(parse_qsl(parsed.query)) > 5:
            return False

        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|ics|ical|ifb|pptx|ppsx|odc|war"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
