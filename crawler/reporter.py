from collections import defaultdict
from urllib.parse import urlparse
import pickle

import scraper

class Reporter(object):
  def __init__(self, restart):
    self.save_file = 'stats.pkl'
    self.freq_file = 'freq.pkl'
    if restart: 
        self.stats = dict()
        self.stats['page_count'] = 0                        # counts number of crawls
        self.stats['longest_page'] = ''
        self.stats['longest_page_words'] = 0
        self.stats['icsSubdomains'] = self.dd()

        self.all_freq = self.dd()
    else:
        self.readSaveFile()

  # to make pickling possible for defaultdicts
  def dd(self):
    return defaultdict(int)

  
  def readSaveFile(self):
    ''' Reads saved data from pickle file. '''
    s_file = open(self.save_file, 'rb')
    self.stats = pickle.load(s_file)

    f_file = open(self.freq_file, 'rb')
    self.all_freq = pickle.load(f_file)

  
  def writeSaveFile(self):
    ''' Writes save data to pickle file. '''
    s_file = open(self.save_file, 'wb')
    pickle.dump(self.stats, s_file)

    f_file = open(self.freq_file, 'wb')
    pickle.dump(self.all_freq, f_file)

  
  def addPage(self, url):
    ''' Increments page count and ics.uci.edu subdomain count if appropriate. '''
    self.stats['page_count'] += 1

    parsed = urlparse(url)
    # record subdomains in ics.uci.edu
    if parsed.netloc.endswith('.ics.uci.edu'):
        self.stats['icsSubdomains'][parsed.scheme + '://' + parsed.netloc] += 1
        print(parsed.scheme + '://' + parsed.netloc, self.stats['icsSubdomains'][parsed.scheme + '://' + parsed.netloc])


  def collect_data(self, tbd_url, page_text):
    ''' Called for every crawled URL, collecting data about the page for the report:
        page count, word frequencies, page length in words, ics.uci.edu subdomains '''
    self.addPage(tbd_url)
    print(self.stats['page_count'])

    # tokenize page text and count frequencies
    wordCount = scraper.tokenFrequencies(page_text, self.all_freq)
    # check if longest page yet
    print('page size in words:', wordCount)
    if wordCount > self.stats['longest_page_words']:
        self.stats['longest_page_words'] = wordCount
        self.stats['longest_page'] = tbd_url
        print('new longest page!', self.stats['longest_page_words'], 'words')

    # report stats every 20 crawls  
    elif self.stats['page_count'] % 20 == 0:
        self.report()
        self.writeReport()

    self.writeSaveFile()


  def report(self):
    ''' Prints report data. '''
    print('---------------------------')
    print('Unique pages:', self.stats['page_count'])
    print('---------------------------')
    print('Longest page:', self.stats['longest_page'])
    print('\thad', self.stats['longest_page_words'], 'words')
    print('---------------------------')
    print('Most common words:')
    stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]
    count = 0
    # sort word frequencies by most frequent to least frequent
    for word in sorted(self.all_freq.keys(), key=(lambda k: self.all_freq[k]), reverse=True):
        if word not in stopwords:  # skip stop words
          print(word)
          count += 1
          if count == 50:
            break
    print('---------------------------')
    print('Subdomains in ics.uci.edu:')
    # sort subdomains alphabetically, print their name and count
    for subdomain in sorted(self.stats['icsSubdomains']):
        print(subdomain + ',', self.stats['icsSubdomains'][subdomain], 'pages')
    print('---------------------------')


  def writeReport(self):
    ''' Writes report data to file. '''
    with open('report.txt', 'w+') as file:
        file.write('---------------------------\n')
        file.write('Unique pages: {}\n'.format(self.stats['page_count']))
        file.write('---------------------------\n')
        file.write('Longest page: {}\n'.format(self.stats['longest_page']))
        file.write('\thad {} words\n'.format(self.stats['longest_page_words']))
        file.write('---------------------------\n')
        file.write('Most common words:\n')
        stopwords = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]
        count = 0
        # sort word frequencies by most frequent to least frequent
        for word in sorted(self.all_freq.keys(), key=(lambda k: self.all_freq[k]), reverse=True):
            if word not in stopwords:  # skip stop words
              file.write('{}\n'.format(word))
              count += 1
              if count == 50:
                break
        file.write('---------------------------\n')
        file.write('Subdomains in ics.uci.edu:\n')
        # sort subdomains alphabetically, print their name and count
        for subdomain in sorted(self.stats['icsSubdomains']):
            file.write('{}, {}\n'.format(subdomain, self.stats['icsSubdomains'][subdomain]))
        file.write('---------------------------\n')

