#!/usr/bin/env python
import random, sys
from splunklib.modularinput import *
import splunklib.client as client
import httplib2
import xml.etree.ElementTree as ET
import logging
import os.path as op
import Queue
import traceback
import time

#import json
sys.path.append(op.join(op.dirname(op.abspath(__file__)), "framework"))

import job_source as js
import job_scheduler as sched
import data_loader as dl
import utils
import log_files
import event_writer
import configure as conf

import hipchat_job_factory as jf
from hipchat_config import (HipChatConfig, HipChatConfMonitor, handle_ckpts)


all_logs = log_files.get_all_logs()
all_logs.append("ta_hipchat")


_LOGGER = utils.setup_logging("ta_hipchat")

class ModinputJobSource(js.JobSource):
	
	def __init__(self, stanza_configs):
		self._done = False
		self._job_q = Queue.Queue()
		self.put_jobs(stanza_configs)

	def put_jobs(self, jobs):
		for job in jobs:
			self._job_q.put(job)

	def get_jobs(self, timeout=0):
		jobs = []
		try:
			while 1:
				jobs.append(self._job_q.get(timeout=timeout))
		except Queue.Empty:
			return jobs


def setup_logging():
	logging.root
	logging.root.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(levelname)s %(message)s')
	handler = logging.StreamHandler(stream=sys.stderr)
	handler.setFormatter(formatter)
	logging.root.addHandler(handler)

def do_scheme():
	"""
	Feed splunkd the TA's scheme
	"""

	print """
	<scheme>
	<title>Splunk Add-on for Box</title>
	<description>Enable Box RESTful inputs</description>
	<use_external_validation>true</use_external_validation>
	<streaming_mode>xml</streaming_mode>
	<use_single_instance>true</use_single_instance>
	<endpoint>
		<args>
			<arg name="name">
				<title>Unique name which identifies this data input</title>
			</arg>
			<arg name="rest_endpoint">
				<title>Box REST endpoint</title>
			</arg>
			<arg name="duration">
				<title>Collection interval for this endpoint (in seconds)</title>
			</arg>
		</args>
	</endpoint>
	</scheme>
	"""
'''
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
'''

def _setup_signal_handler(data_loader):
	"""
	Setup signal handlers
	@data_loader: data_loader.DataLoader instance
	"""

	def _handle_exit(signum, frame):
		_LOGGER.info("Box TA is going to exit...")
		data_loader.tear_down()

	utils.handle_tear_down_signals(_handle_exit)

def _get_hipchat_configs():
	#set up logging
	#_setup_logging()
	#create box config object
	hipchat_conf = HipChatConfig()
	meta_configs, stanza_configs = hipchat_conf.get_configs()
	#re set up logging if it is different...
	if stanza_configs:
		loglevel = stanza_configs[0].get("loglevel","INFO")
		if loglevel != "INFO":
			print "SET UP LOG..."
			#_setup_logging(loglevel, True)
	else:
		print "No data collection for hipchat found"
		#_LOGGER.info("No data collection for box is found in the "
        #             "inputs.conf. Do nothing and Quit the TA")
        #return None, None

    #Require all defaults are present otherwise app is not porperly configured.
	return meta_configs, stanza_configs

def _get_file_change_handler(data_loader, meta_configs):
	def reload_and_exit(changed_files):
		#_LOGGER.info("Reload conf %s", changed_files)
		print "Reload conf %s", changed_files
		conf.reload_confs(changed_files, meta_configs["session_key"], meta_configs["server_uri"])
		data_loader.tear_down()

	return reload_and_exit


def run():
	try:
		#meta_configs, stanza_configs = _get_hipchat_configs()
		meta_configs = {'server_uri': u'https://127.0.0.1:8089', 'server_host': u'zshainsky-mbpr.local', 'session_key': u'kB3aBRF94mvJnkItzMhmgt8napohyCNqXTNhJyyBDh7LWUIyrWWfH_VQfRASnAuPCBT7fotJavQmJl9MQlh2B4OCJeuvylSH21vffwHHZHDJk1525naDk13', 'checkpoint_dir': u'/opt/versions/Splunk_ta_project/var/lib/splunk/modinputs/hipchat_service'}
		stanza_configs = [{u'host': u'zshainsky-mbpr.dfw.splunk.com', u'rest_endpoint': u'http://api.hipchat.com/v1/users/list?format=json&auth_token=e53255c943a9f133c75e3457819371', u'disabled': u'0', u'index': u'hipchat', 'name': u'hipchat_service://Bleh', u'duration': u'30'}]

		print meta_configs, stanza_configs
		print type(meta_configs), type(stanza_configs)
	except Exception as ex:
		_LOGGER.error("Failed to setup config for HipChat TA: %s", ex.message)
		_LOGGER.error(traceback.format_exc())
		raise

	if not stanza_configs:
		print "Return: not Stanza_Configs1"
		return

	writer = event_writer.EventWriter()
	job_src = ModinputJobSource(stanza_configs)
	job_factory = jf.HipChatJobFactory(job_src, writer)
	job_scheduler = sched.JobScheduler(job_factory)
	data_loader = dl.GlobalDataLoader.get_data_loader(stanza_configs,job_scheduler,writer)
	print data_loader.settings, data_loader.configs
	callback = _get_file_change_handler(data_loader, meta_configs)
	conf_monitor = HipChatConfMonitor(callback)
	data_loader.add_timer(conf_monitor.check_changes, time.time(), 60)
	_setup_signal_handler(data_loader)
	_LOGGER.info("Starting data_loader.run()...")
	data_loader.run()
	_LOGGER.info("Finished data_loader.run()!!!")

'''
	http = httplib2.Http(".cache")

	event = Event()
	event.index = "test_the_rest"
	event.sourceType = "json"

	for input_name, input_item in inputs.inputs.iteritems():
		rest_endpoint = input_item["rest_endpoint"]
		(resp_headers, content) = http.request(rest_endpoint, "GET")

		event.stanza = input_name
		event.data = content
		ew.write_event(event)


def stream_events(self, inputs, ew):
	http = httplib2.Http(".cache")
	event = Event()
	event.index = "test_the_rest"
	event.sourceType = "json"
	for input_name, input_item in inputs.inputs.iteritems():
		rest_endpoint = input_item["rest_endpoint"]
		(resp_headers, content) = http.request(rest_endpoint, "GET")

		event.stanza = input_name
		event.data = content
		ew.write_event(event)
'''

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


def usage():
	"""
	Print usage of this binary
	"""

	hlp = "%s --scheme|--validate-arguments|-h"
	print >> sys.stderr, hlp % sys.argv[0]
	sys.exit(1)


def main():
	"""
	Main entry point
	"""

	args = sys.argv
	if len(args) > 1:
	    if args[1] == "--scheme":
	        do_scheme()
	    elif args[1] == "--validate-arguments":
	        sys.exit(validate_config())
	    elif args[1] in ("-h", "--h", "--help"):
	        usage()
	    else:
	        usage()
	else:
	    #_LOGGER.info("Start Box TA")
	    print ("Starting HipChat TA")
	    run()
	    print ("End HipChat TA")
	    #_LOGGER.info("End Box TA")
	sys.exit(0)


if __name__ == "__main__":
	main()
	#sys.exit(ModinputHipchat().run(sys.argv))

