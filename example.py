import sys
import time

from fetcher import fetch


def get_requests(count, url):
  print 'GETting %s from %s' % (count, url)
  return ((url, 'request-%s' % i) for i in range(count))


def post_requests(count, url):
  print 'POSTting %s from %s' % (count, url)
  return ((url, i, 'request=%s' % i) for i in range(count))


def make_requests(requests):
  start = time.time()
  for ok, resp in fetch(requests, concurrent=100):
    print ok, resp
  delta = time.time() - start
  print '%.02f req/s' % (count / delta)

if __name__ == '__main__':
  count = int(sys.argv[1])
  url   = sys.argv[2]
  requests_method = post_requests if sys.argv[3:] == ['POST'] else get_requests
  sys.exit(make_requests(requests_method(count, url)))
