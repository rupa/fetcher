import pycurl
import sys
import time

from cStringIO import StringIO


def post(requests, opts):
  """
  Perform POST requests. Uses opts dictionary so we can easily pass arbitrary
  options to pycurl.setopt.
  """
  options = {
    'concurrent': opts.pop('concurrent', 50),
    'timeout_ms': opts.pop('timeout_ms', 1000),
    'follow_redirects': opts.pop('follow_redirects', True),
  }
  options.update(opts)
  return _multi_curl(_post, requests, options)


def fetch(requests, concurrent=50, timeout_ms=1000, follow_redirects=True):
  """
  Perform GET requests. Currently no provision to pass extra options.
  """
  options = {
    'concurrent': concurrent,
    'timeout_ms': timeout_ms,
    'follow_redirects': follow_redirects,
  }
  return _multi_curl(_get, requests, options)


def _get(requests):
  """
  pop a GET request off a stack of (URL, echo_field)
  """
  try:
    url, payload = requests.next()
  except StopIteration:
    return None
  curl = pycurl.Curl()
  curl.setopt(pycurl.URL, url)
  curl.payload = payload
  return curl


def _post(requests):
  """
  pop a POST request off a stack of (URL, post_data)
  """
  try:
    url, post_data = requests.next()
  except StopIteration:
    return None
  curl = pycurl.Curl()
  curl.setopt(pycurl.URL, url)
  curl.setopt(pycurl.POSTFIELDS, post_data)
  curl.payload = post_data
  return curl


def _multi_curl(func, requests, options):

  # required opts, remaining opts are passed to pycurl.setopt
  concurrent       = options.pop('concurrent')
  timeout_ms       = options.pop('timeout_ms')
  follow_redirects = options.pop('follow_redirects')

  multi = pycurl.CurlMulti()

  # Sadly, we need to track of pending curls, or they'll get GC'd and
  # mysteriously disappear. Don't ask me!
  curls            = []
  num_handles       = 0
  unscheduled_reqs = True

  while num_handles or unscheduled_reqs or curls:
    # If the concurrency cap hasn't been reached yet, another request can be
    # pulled off and added to the multi.
    if unscheduled_reqs and num_handles < concurrent:

      # pull a request off the generator and build a curl
      curl = func(requests)

      if curl is None:
        unscheduled_reqs = False
      else:
        body = StringIO()
        curl.setopt(pycurl.WRITEFUNCTION, body.write)
        curl.body = body
        curl.setopt(pycurl.TIMEOUT_MS, timeout_ms)
        curl.setopt(pycurl.CONNECTTIMEOUT_MS, timeout_ms)
        curl.setopt(pycurl.FOLLOWLOCATION, 1 if follow_redirects else 0)
        curl.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64;'
          ' rv:21.0) Gecko/20100101 Firefox/21.0')
        # set any extra options
        for curlopt, value in options.items():
          curl.setopt(curlopt, value)

        curls.append(curl)
        multi.add_handle(curl)

    # Perform any curl requests that need to happen.
    ret = pycurl.E_CALL_MULTI_PERFORM
    while ret == pycurl.E_CALL_MULTI_PERFORM:
      ret, num_handles = multi.perform()

    # Wait at maximum for two seconds for a file descriptor to become available.
    # Restart if not.
    ret = multi.select(2.0)
    if ret == -1:
      continue

    # Finally, deal with any complete or error'd curls that may have been
    # resolved in this loop.
    while True:
      num_q, ok_list, err_list = multi.info_read()
      for c in ok_list:
        yield True, (c.payload, c.body.getvalue())
        multi.remove_handle(c)
        curls.remove(c)

      for c, errno, errmsg in err_list:
        error = "%d: %s" % (errno, errmsg)
        yield False, (c.payload, error, c.getinfo(pycurl.EFFECTIVE_URL))
        multi.remove_handle(c)
        curls.remove(c)

      if not num_q:
        break


def _example_POST(count, url):
  """
  Note arguments in options dictionary
  """
  print 'POSTing %s from %s' % (count, url)

  requests = ((url, 'req_id=%s' % i) for i in range(count))
  options = {'concurrent': 100}

  start = time.time()
  for ok, resp in post(requests, options):
    print ok         # error flag
    print resp[0]    # echo of POST data
    if ok:
      print resp[1]  # server response
    else:
      print resp[1]  # error message
      print resp[2]  # error URL (we may have followed redirects)
  delta = time.time() - start
  print '%.02f req/s' % (count / delta)


def _example_GET(count, url):
  """
  Note keyword arguments
  """
  print 'GETting %s from %s' % (count, url)

  requests = ((url, 'req-%s' % i) for i in range(count))
  start = time.time()
  for ok, resp in fetch(requests, concurrent=100):
    print ok         # error flag
    print resp[0]    # echo field
    if ok:
      print resp[1]  # server response
    else:
      print resp[1]  # error message
      print resp[2]  # error URL (we may have followed redirects)
  delta = time.time() - start
  print '%.02f req/s' % (count / delta)


if __name__ == '__main__':
  count = int(sys.argv[1])
  url   = sys.argv[2]
  sys.exit(_example_GET(count, url))
