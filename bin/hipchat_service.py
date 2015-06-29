#!/usr/bin/python
import random, sys
from splunklib.modularinput import *
import splunklib.client as client
import httplib2
import xml.etree.ElementTree as ET
import logging
import time
#import json

import multiprocessing
import traceback
import Queue

from apscheduler.apscheduler.scheduler.background import BackgroundScheduler
from datetime import datetime

def tick():
	print('Tick! The time is: %s' % datetime.now())


class ModinputHipchat(Script):
	def setup_logging():
		logging.root
		logging.root.setLevel(logging.DEBUG)
		formatter = logging.Formatter('%(levelname)s %(message)s')
		handler = logging.StreamHandler(stream=sys.stderr)
		handler.setFormatter(formatter)
		logging.root.addHandler(handler)

	def get_scheme(self):
		scheme = Scheme("Hipchat Modular Input")
		scheme.description="Streams Hipchat events from specified chatrooms"
		scheme.use_external_validation = True
		scheme.use_single_instance = True

		rest_endpoint_arg = Argument("rest_endpoint")
		rest_endpoint_arg.data_type = Argument.data_type_string
		rest_endpoint_arg.description = "Hipchat REST endpoint"
		rest_endpoint_arg.required_on_create = True
		scheme.add_argument(rest_endpoint_arg)

		duration_arg = Argument("duration")
		duration_arg.data_type = Argument.data_type_number
		duration_arg.description = "Collection interval for this endpoint (in seconds)"
		duration_arg.required_on_create = True
		scheme.add_argument(duration_arg)

		return scheme

	def validate_input(self, validation_definition):
		# name = validation_definition.parameters["name"]
		rest_endpoint = validation_definition.parameters["rest_endpoint"]
		duration = float(validation_definition.parameters["duration"])

		if rest_endpoint == " ":
			# print "rest_endpoint..."
			raise ValueError("Rest Endpoint field must have a value")

		if duration == " ":
			# print "duration..."
			raise ValueError("Duration field must have a value")
        

	def stream_events(self, inputs, ew):
		http = httplib2.Http(".cache")
		scheduler = BackgroundScheduler()
		scheduler.start()

		event = Event()
		event.index = "hipchat"
		event.sourceType = "json"

		print "number of inputs:",len(inputs)
		while 1:
			for input_name, input_item in inputs.inputs.iteritems():
				# Spin up a thread for each input --- Why do we have queues??
				# Sleep for input_item["duration"]
				# Requery for data
				# Sleep...
				print input_item
				rest_endpoint = input_item["rest_endpoint"]
				(resp_headers, content) = http.request(rest_endpoint, "GET")

			event.stanza = input_name
			event.data = content
			ew.write_event(event)
			print "Sleeping...."
			time.sleep(10)
			print "Done Sleeping!!"


if __name__ == "__main__":
	sys.exit(ModinputHipchat().run(sys.argv))
