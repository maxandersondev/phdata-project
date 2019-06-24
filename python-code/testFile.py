import re

LOG_REGEX = '(?P<ip>[(\d\.)]+) - - \[(?P<date>.*?) -(.*?)\] "(?P<method>\w+) (?P<request_path>.*?) HTTP/(?P<http_version>.*?)" (?P<status_code>\d+) (?P<response_size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'

line = '172.183.134.216 - - [12/Jul/2016:12:22:14 -0700] "GET /wp-content HTTP/1.0" 200 4980 "http://farmer-harris.com/category/index/" "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; rv:1.9.3.20) Gecko/2013-07-10 02:46:11 Firefox/9.0"'
line = '209.112.63.162 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; {1C69E7AA-C14E-200E-5A77-8EAB2D667A07})"'
line = '155.157.99.22 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/5.0 (Windows; U; Windows NT 5.0; ja-JP; rv:1.7.12) Gecko/20050919 Firefox/1.0.7"'
compiled = re.compile(LOG_REGEX)

match = compiled.match(line.encode())
data = match.groupdict()
print(data)