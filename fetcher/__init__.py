import pycurl

from cStringIO import StringIO


def fetch(requests, concurrent=50, timeout_ms=1000, follow_redirects=True,
          curlopts=None):
  """
  requests argument is a generator with the following structure:

    (url, echo_field)            - for GET requests
    (url, echo_field, post_data) - for POST requests

  curlopts allows arbitrary options to be passed to pycurl.setopt. It is a list
  of two-tuples, eg:

    (pycurl.HTTPHEADER, ['Content-Type', 'application/javascript'])

  responses:
    success: (True, (echo_field, server_response))
      error: (False, (echo_field, error, effective_URL))
  """
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

      try:
        request = requests.next()
      except StopIteration:
        unscheduled_reqs = False
        continue

      if len(request) == 3:
        url, payload, post_data = request
      elif len(request) == 2:
        url, payload = request
        post_data = None
      else:
        raise Exception('Bad request: {}'.format(repr(request)))

      body = StringIO()

      curl = pycurl.Curl()
      curl.setopt(pycurl.URL, url)
      curl.setopt(pycurl.WRITEFUNCTION, body.write)
      curl.setopt(pycurl.TIMEOUT_MS, timeout_ms)
      curl.setopt(pycurl.CONNECTTIMEOUT_MS, timeout_ms)
      curl.setopt(pycurl.FOLLOWLOCATION, 1 if follow_redirects else 0)
      curl.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64;' +\
        ' rv:21.0) Gecko/20100101 Firefox/21.0')

      # arbitrary options
      if curlopts is not None:
        for option, value in curlopts:
          curl.setopt(option, value)

      if post_data is not None:
        curl.setopt(pycurl.POSTFIELDS, post_data)

      curl.body    = body
      curl.payload = payload
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
