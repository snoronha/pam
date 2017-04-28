import time
import pandas as pd
import numpy
from dateutil.parser import *
import re

iters = 100000

date_regex = re.compile(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2}) (\d{1,2}):(\d{1,2}):(\d{1,2})')
start = time.time()
for x in range(0, iters):
    dt     = '2014-11-13 11:12:53'
    mdate  = date_regex.match(dt)
    year   = mdate.group(1)
    month  = mdate.group(2)
    day    = mdate.group(3)
    hour   = mdate.group(4)
    minute = mdate.group(5)
    sec    = mdate.group(6)
    time_tuple = time.struct_time((year,month,day,hour,minute,sec,0,0,0))
end = time.time()    
print "%d time.struct_time execution: %f" % (iters, (end - start))

start = time.time()
for x in range(0, iters):
    struct_time = time.strptime("30-11-2012 11:12:53", "%d-%m-%Y %H:%M:%S")
end = time.time()    
print "%d strptime execution: %f" % (iters, (end - start))

start = time.time()
for x in range(0, iters):
    date_time = pd.to_datetime('30-11-2012 11:12:53', errors='ignore')
end = time.time()    
print "%d pd.to_datetime execution (no format): %f" % (iters, (end - start))

start = time.time()
for x in range(0, iters):
    date_time = pd.to_datetime('30-11-2012 11:12:53', format='%d-%m-%Y %H:%M:%S', errors='ignore')
end = time.time()    
print "%d pd.to_datetime execution: %f" % (iters, (end - start))

start = time.time()
for x in range(0, iters):
    date_time = parse('30-11-2012 11:12:53')
end = time.time()    
print "%d dateutil.parse execution: %f" % (iters, (end - start))
