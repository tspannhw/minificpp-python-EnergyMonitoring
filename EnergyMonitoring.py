#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Install:  nifi-minifi-cpp-0.6.0/minifi-python

import codecs
from pyHS100 import Discover
import json
import datetime
import time
import uuid
from time import gmtime, strftime
import psutil


def describe(processor):
    processor.setDescription("Source Processor ingests energy data")


def onInitialize(processor):
    processor.setSupportsDynamicProperties()


class EnergyMonitoring(object):
    def __init__(self):
        self.content = None

    def process(self, input_stream):
        self.content = codecs.getreader('utf-8')(input_stream).read()
        return len(self.content)

    def onTrigger(context, session):
        flow_file = session.get()
        if flow_file is not None:
            for dev in Discover.discover().values():
                now = datetime.datetime.now()
                row = {}
                year = now.year
                month = now.month
                start = time.time()
                sysinfo = dev.get_sysinfo()
                uuid2 = '{0}_{1}'.format(strftime("%Y%m%d%H%M%S", gmtime()), uuid.uuid4())

                for k, v in dev.get_emeter_realtime().items():
                    row["%s" % k] = v

                for k, v in sysinfo.items():
                    row["%s" % k] = v

                emeterdaily = dev.get_emeter_daily(year=year, month=month)

                for k, v in emeterdaily.items():
                    row["day%s" % k] = v

                hwinfo = dev.hw_info

                for k, v in hwinfo.items():
                    row["%s" % k] = v

                timezone = dev.timezone

                for k, v in timezone.items():
                    row["%s" % k] = v

                emetermonthly = dev.get_emeter_monthly(year=year)

                for k, v in emetermonthly.items():
                    row["month%s" % k] = v

                row['host'] = dev.host
                row['current_consumption'] = dev.current_consumption()
                row['alias'] = dev.alias
                row['devicetime'] = dev.time.strftime('%m/%d/%Y %H:%M:%S')
                row['ledon'] = dev.led
                end = time.time()
                row['end'] = '{0}'.format(str(end))
                row['te'] = '{0}'.format(str(end - start))
                row['systemtime'] = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
                row['cpu'] = psutil.cpu_percent(interval=1)
                row['memory'] = psutil.virtual_memory().percent
                usage = psutil.disk_usage("/")
                row['diskusage'] = "{:.1f}".format(float(usage.free) / 1024 / 1024)
                row['uuid'] = str(uuid2)
                # Output JSON
                json_string = json.dumps(row)
                print(json_string)
                flow_file.addAttribute("row", str(json_string))
                flow_file.addAttribute("host", str(row['host']))
                #flow_file.addAttribute("negative", str(vs['neg']))
                #flow_file.addAttribute("neutral", str(vs['neu']))

        session.transfer(flow_file, REL_SUCCESS)
