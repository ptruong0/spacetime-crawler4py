from collections import defaultdict
from urllib.parse import urlparse

import scraper

class Reporter(object):
  def __init__(self):
    self.page_count = 0                           # counts number of crawls
    self.longest_page = None                      
    self.longest_page_words = 0
    self.all_freq = defaultdict(int)
    self.icsSubdomains = defaultdict(int)

  def addPage(self):
    self.page_count += 1

  def collect_data(self, tbd_url, page_text):
    parsed = urlparse(tbd_url)
    self.addPage()
    print(self.page_count)

    # tokenize page text and count frequencies
    wordCount = scraper.tokenFrequencies(page_text, self.all_freq)
    # check if longest page yet
    if wordCount > self.longest_page_words:
        self.longest_page_words = wordCount
        self.longest_page = tbd_url
        print('new longest page!', self.longest_page_words, 'words')

    # record subdomains in ics.uci.edu
    if parsed.netloc.endswith('.ics.uci.edu'):
        self.icsSubdomains[parsed.scheme + '://' + parsed.netloc] += 1
        print(parsed.scheme + '://' + parsed.netloc, self.icsSubdomains[parsed.scheme + '://' + parsed.netloc])

    # report stats every 10 crawls
    if self.page_count % 10 == 0:
        self.report()

  def report(self):
        print('---------------------------')
        print('Unique pages:', self.page_count)
        print('---------------------------')
        print('Longest page:', self.longest_page)
        print('\thad', self.longest_page_words, 'words')
        print('---------------------------')
        print('Most common words:')
        stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]
        count = 0
        # sort word requencies by most frequent to least frequent
        for word in sorted(self.all_freq.keys(), key=(lambda k: self.all_freq[k]), reverse=True):
            if word not in stopwords:  # skip stop words
              print(word)
              count += 1
              if count == 50:
                break
        print('---------------------------')
        print('Subdomains in ics.uci.edu:')
        # sort subdomains alphabetically, print their name and count
        for subdomain in sorted(self.icsSubdomains):
            print(subdomain + ',', self.icsSubdomains[subdomain], 'pages')
        print('---------------------------')

