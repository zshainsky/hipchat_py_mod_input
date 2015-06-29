import os.path as op
import sys
import traceback
import logging
import urllib
import threading
import time
import Queue
from datetime import (datetime, timedelta)
from json import loads

sys.path.append(op.join(op.dirname(op.abspath(__file__)), "framework"))

import hipchat_job_factory as jf
import configure as conf
import rest


#_LOGGER = logging.getLogger("ta_box")


def _json_loads(content):
    if not content:
        return None

    try:
        return loads(content)
    except Exception:
        _LOGGER.error("Failed to parse json. Reason=%s",
                      traceback.format_exc())
        return None


def flatten_json_dict_type(prefix, obj):
    template = '%s_{}="{}"' % prefix if prefix else '{}="{}"'
    results = []
    for k, v in obj.iteritems():
        if isinstance(v, dict):
            if prefix:
                k = "{}_{}".format(prefix, k)
            res = flatten_json_dict_type(k, v)
            if res:
                res = ",".join(res)
        elif isinstance(v, list):
            # FIXME
            res = template.format(k, v)
        else:
            res = template.format(k, v if v is not None else "")
        results.append(res)
    return results


def _flatten_box_json_object(json_obj):
    results = []
    if "entries" in json_obj:
        for obj in json_obj["entries"]:
            res = flatten_json_dict_type(None, obj)
            if res:
                results.append(",".join(res))
    else:
        if isinstance(json_obj, dict):
            res = flatten_json_dict_type(None, json_obj)
            if res:
                results.append(",".join(res))
        else:
            assert False
    return results


class _NewTokens(object):
    access_token = None
    refresh_token = None
    __lock = threading.Lock()

    @staticmethod
    def get_tokens():
        with _NewTokens.__lock:
            return (_NewTokens.access_token, _NewTokens.refresh_token)

    @staticmethod
    def set_tokens(access_token, refresh_token):
        with _NewTokens.__lock:
            _NewTokens.access_token = access_token
            _NewTokens.refresh_token = refresh_token


class _JObject(object):

    def __init__(self, obj, url, endpoint, loader):
        self.obj = obj
        self.url = url
        self.endpoint = endpoint
        self.loader = loader

    def to_string(self, idx, host):
        evt_fmt = ("<event><source>{0}</source><sourcetype>{1}</sourcetype>"
                   "<host>{2}</host><index>{3}</index>"
                   "<data><![CDATA[ {4} ]]></data></event>")
        results = _flatten_box_json_object(self.obj)

        def evt_join(res):
            return evt_fmt.format(self.url, "box:" + self.endpoint,
                                  host, idx, res)

        data = "".join((evt_join(res) for res in results))
        return data


class HipChatBase(object):

    def __init__(self, config):
        """
        @config: dict like, should have url, refresh_token, access_token,
                 checkpoint_dir, proxy_url, proxy_username, proxy_password
        """

        #self.access_token = config["access_token"]
        #self.config = config
        #self.conf_mgr = conf.ConfManager(self.config["server_uri"],
        #                                 self.config["session_key"])
        self._lock = threading.Lock()
        self._stopped = False
        self.headers = {
            "Accept-Encoding": "gzip",
            "Accept": "application/json"#,
            #"Authorization": "Bearer {0}".format(config["access_token"]),
        }
        self.http = None
        print "PRINTING SELF FROM HIPCHAT_DATA_LOADER.py:", self

        import data_loader as dl
        self._loader = dl.GlobalDataLoader.get_data_loader(None, None, None)

    def collect_data(self):
        if self._lock.locked():
            _LOGGER.info("Last request for endpoint=%s has not been done yet",
                         self.config["rest_endpoint"])
            return True, None

        if self.http is None:
            self.http = rest.build_http_connection(self.config)

        ret = self._check_if_job_is_due()
        if not ret:
            return True, None

        if self._shutdown():
            return True, None

        with self._lock:
            done, objs = self._do_collect()
            return done, objs

    def _do_collect(self):
        """
        @return: (done, objs)
        """

        uri = self._get_uri()
        results = []
        err, content = self._send_request(uri, results,
                                          self.config["rest_endpoint"])
        if err:
            return True, None

        self._save_ckpts(content)

        if not results:
            return True, results

        return False, results

    def _check_if_job_is_due(self):
        """
        @return: True if the job is due else return False
        """

        if not self._do_expiration_check():
            return True

        ckpt = self._get_state()
        if ckpt is not None and ckpt["ckpts"] is None:
            last_collect_time = ckpt["timestamp"]
            delta = last_collect_time + self.config["duration"] - time.time()
            if delta > 0:
                _LOGGER.info("There are %f seconds for the job=%s to be due",
                             delta, self.config["rest_endpoint"])
                return False 
        return True

    def _do_expiration_check(self):
        return True 

    def _get_uri(self, ckpt=None, option=None):
        pass

    def _save_ckpts(self, content):
        pass

    def _save_new_access_token(self, content):
        content = _json_loads(content)
        self.refresh_token = content["refresh_token"]
        self.access_token = content["access_token"]
        self.headers["Authorization"] = "Bearer {0}".format(
            content["access_token"])
        _NewTokens.set_tokens(content["access_token"], self.refresh_token)
        keys = ("access_token", "refresh_token")
        content = {key: content[key] for key in keys}
        res = self.conf_mgr.update_conf_properties(
            user="nobody", appname=self.config["appname"], file_name="box",
            stanza="box_account", key_values=content)

        if res is False:
            _LOGGER.error("Failed to update a acces_token")
        return res

    def _shutdown(self):
        return self._loader.stopped()

    def _handle_token_refresh(self):
        _LOGGER.info("Access token has been expired, refreshing")
        new_tokens = _NewTokens.get_tokens()
        if new_tokens[0] is not None:
            if new_tokens != (self.access_token, self.refresh_token):
                self.access_token = new_tokens[0]
                self.refresh_token = new_tokens[1]
                _LOGGER.info("Pickup the new tokens.")
                return

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }
        payload = urllib.urlencode(payload)
        oauth_uri = "".join((self.config["url"] + "/oauth2/token"))
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        resp, content = self._do_rest(oauth_uri, headers, "POST", payload)
        if resp and resp.status in (200, 201) and content:
            self._save_new_access_token(content)
        else:
            _LOGGER.error("Failed to refresh access token.")
        _LOGGER.info("End of refreshing access token.")

    def _do_rest(self, rest_uri, headers, method="GET", payload=None):
        _LOGGER.debug("start %s", rest_uri)
        resp, content = None, None
        try:
            resp, content = self.http.request(rest_uri, method=method,
                                              headers=headers, body=payload)
        except Exception:
            _LOGGER.error("Failed to connect %s, reason=%s",
                          rest_uri, traceback.format_exc())
        else:
            if resp.status not in (200, 201):
                _LOGGER.error("Failed to connect %s, reason=%s, %s",
                              rest_uri, resp.reason, content)
                if resp.status == 401:
                    self._handle_token_refresh()
                elif resp.status == 429:
                    _LOGGER.warn("Too many requests to Box.")
        _LOGGER.debug("end %s", rest_uri)
        return resp, content

    def _send_request(self, uri, results, sourcetype, verify_entries=True,
                      index_result=True, init_time=2):
        err, sleep_time = 1, init_time
        valid, result = True, None
        for _ in xrange(6):
            resp, content = self._do_rest(uri, self.headers)
            if resp and resp.status in (200, 201):
                err = 0
                result = _json_loads(content)
                if verify_entries:
                    if not result or not result["entries"]:
                        valid = False

                if valid and index_result:
                    results.append(
                        _JObject(result, self.config["url"],
                                 sourcetype, self._loader))
                break
            elif not resp:
                time.sleep(2)
                self.http = rest.build_http_connection(self.config)
            elif resp.status == 429:
                _LOGGER.warn("Throttling %d seconds", sleep_time)
                time.sleep(sleep_time)
                sleep_time = sleep_time * 2
            elif resp.status == 401:
                err = 401
                self._stopped = True
                break
            elif resp.status in (403, 404, 405, 500, 503):
                err = resp.status
                break
        return err, result

    def _save_state(self, ckpts):
        ckpt = {
            "version": 2,
            "ckpts": ckpts,
            "timestamp": time.time(),
        }
        self.config["state_store"].update_state(self.config["rest_endpoint"],
                                                ckpt)

    def _remove_state(self):
        self.config["state_store"].delete_state(self.config["rest_endpoint"])

    def _get_state(self):
        ckpt = self.config["state_store"].get_state(
            self.config["rest_endpoint"])

        if ckpt:
            assert ckpt["version"] >= 1
            return ckpt
        return None

    @staticmethod
    def is_alive():
        return 1


class BoxFile(HipChatBase):

    def __init__(self, uri, src_type, verify_entries, config):
        super(BoxFile, self).__init__(config)
        self._uri = uri
        self._source_type = src_type
        self._verify_entries = verify_entries

    def _do_expiration_check(self):
        return False 

    def _do_collect(self):
        results = []
        self._send_request(self._uri, results, self._source_type,
                           self._verify_entries)
        return True, results


class HipChatUsers(HipChatBase):
    time_fmt = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, config):
        super(HipChatUsers, self).__init__(config)
        self._last_created_before = None

    def _do_expiration_check(self):
        return False 

    def _get_ckpts(self):
        ckpt = self._get_state()
        if ckpt is None:
            after = datetime.strptime(self.config["created_after"],
                                      self.time_fmt)
            before = after + timedelta(hours=24)
            before = datetime.strftime(before, self.time_fmt)
            return {
                "created_after": self.config["created_after"],
                "created_before": before,
                "stream_position": 0,
            }
        else:
            before = datetime.utcnow()
            after = before - timedelta(seconds=self.config["duration"])
            before = datetime.strftime(before, self.time_fmt)
            after = datetime.strftime(after, self.time_fmt)
            if ckpt["version"] == 1:
                return {
                    "created_after": after,
                    "created_before": before,
                    "stream_position": ckpt["ckpts"],
                }

            if ckpt["ckpts"]["created_before"] == "now":
                now = datetime.strftime(datetime.utcnow(), self.time_fmt)
                ckpt["ckpts"]["created_before"] = now
            return ckpt["ckpts"]

    def _save_ckpts(self, result):
        # Only when there are results from Box, we progress the created_after
        # 1) (created_before + 1 day <= now)
        #    we are still collecting history data
        # 2) (created_before + 1 day > now)
        #    we are collecting the latest data, created_before is always "now"
        # 3) (created_after + 1 day < now and created_before + 1 day > now)
        #    we are colllecting the latest data but Box API doesn't respond
        #    us with any events more than a day, Box is probably messed up
        #    with next_stream_position or Box TA has been shutdown for more
        #    than 1 day, rewind to 0 to recover next_stream_pos

        ckpts = self._get_ckpts()
        now = datetime.utcnow()
        created_after = datetime.strptime(ckpts["created_after"],
                                          self.time_fmt)
        created_before = datetime.strptime(self._last_created_before,
                                           self.time_fmt)
        aday = timedelta(hours=24)
        ckpts["stream_position"] = result["next_stream_position"]

        if created_before + aday > now:
            if (result["entries"] and
                    len(result["entries"]) < self.config["record_count"]):
                ckpts["created_after"] = self._last_created_before
            else:
                if created_after + aday < now:
                    ckpts["stream_position"] = "0"
                    _LOGGER.warn("Rewind stream posistion to 0 to recover "
                                 "stream position")
            ckpts["created_before"] = "now"
        elif created_before + aday <= now:
            if len(result["entries"]) < self.config["record_count"]:
                # We are done with history events in this windows.
                # Move to next time window
                ckpts["created_after"] = self._last_created_before
                next_before = datetime.strftime(created_before + aday,
                                                self.time_fmt)
                ckpts["created_before"] = next_before
                _LOGGER.debug("Progress to created_before=%s, pos=%s",
                              next_before, ckpts["stream_position"])
        self._save_state(ckpts)

    def _get_uri(self, ckpt=None, option=None):
        ckpts = self._get_ckpts()
        self._last_created_before = ckpts["created_before"]
        '''
        params = ("?stream_type=admin_logs&limit={}&stream_position={}"
                  "&created_after={}-00:00&created_before={}-00:00").format(
                      self.config["record_count"], ckpts["stream_position"],
                      ckpts["created_after"], ckpts["created_before"])
        url = "".join((self.config["restapi_base"], "/events", params))
        '''
        url = self.config["rest_endpoint"]

        print "URL (_get_uri in data_loader:",url
        _LOGGER.info("Get %s", url)
        return url


class BoxUserGroupBase(HipChatBase):

    def __init__(self, config):
        super(BoxUserGroupBase, self).__init__(config)

    def _get_ckpts(self):
        """
        @return: offset
        """

        ckpt = self._get_state()
        if ckpt is None or ckpt["ckpts"] is None:
            return 0
        else:
            return ckpt["ckpts"]

    def _save_ckpts(self, result):
        if not result or not result["entries"]:
            _LOGGER.info("All %s records have been collected",
                         self.config["rest_endpoint"])
            self._save_state(None)
            return

        if result["total_count"] > result["offset"] + len(result["entries"]):
            offset = result["offset"] + len(result["entries"])
        else:
            offset = result["total_count"]
        self._save_state(offset)


class BoxUser(BoxUserGroupBase):

    def _get_uri(self, ckpt=None, option=None):
        offset = self._get_ckpts()
        params = "?limit={}&offset={}&fields={}".format(
            self.config["record_count"], offset, self.config["user_fields"])
        return "".join((self.config["restapi_base"], "/users", params))


class BoxGroup(BoxUserGroupBase):

    def _get_uri(self, ckpt=None, option=None):
        offset = self._get_ckpts()
        params = "?limit={}&offset={}".format(
            self.config["record_count"], offset)
        return "".join((self.config["restapi_base"], "/groups", params))


class BoxFolder(HipChatBase):
    count_threshhold = 10000
    time_threshhold = 10 * 60

    def __init__(self, config):
        super(BoxFolder, self).__init__(config)

        self._file_count = 0
        self._folder_count = 0
        self._file_total_count = 0
        self._folder_total_count = 0
        self._task_count = 0
        self._collaboration_count = 0

    def _get_uri(self, ckpt=None, option=None):
        if ckpt["type"] == "folder":
            if option is None:
                params = "/folders/{}/items?limit={}&offset={}&fields={}"
                params = params.format(
                    ckpt["id"], self.config["record_count"],
                    int(ckpt.get("offset", 0)), self.config["folder_fields"])
            elif option == "collaborations":
                params = "/folders/{}/collaborations?fields={}".format(
                    ckpt["id"], self.config["collaboration_fields"])
            else:
                assert 0
        else:
            if option is None:
                params = "/files/{}?fields={}".format(
                    ckpt["id"], self.config["file_fields"])
            elif option == "tasks":
                params = "/files/{}/tasks?fields={}".format(
                    ckpt["id"], self.config["task_fields"])
            elif option == "versions":
                params = "/files/{}/versions".format(ckpt["id"])
            elif option == "comments":
                params = "/files/{}/comments".format(
                    ckpt["id"], self.config["comment_fields"])
            else:
                assert 0
        uri = "".join((self.config["restapi_base"], params))
        return uri

    def _get_ckpts(self):
        ckpt = self._get_state()
        if not ckpt or ckpt["ckpts"] is None:
            ckpts = [{"type": "folder", "id": 0, "offset": 0}]
            _LOGGER.info("Start from root")
        else:
            ckpts = ckpt["ckpts"]
            _LOGGER.info("Pickup from %s", ckpts[-1])
        return ckpts

    def _handle_file(self, uri, ckpts, results):
        if self.config["collect_file"]:
            self._file_count += 1
            if not self._run_job_async(uri, "file", False):
                err, _ = self._send_request(uri, results, "file", False)
                if err:
                    ckpts.pop(-1)
                    return

        # Collect tasks/comments for this file
        if self.config["collect_task"]:
            tasks = (("tasks", "fileTask"), ("comments", "fileComment"))
            for endpoint, src_type in tasks:
                uri = self._get_uri(ckpts[-1], endpoint)
                if not self._run_job_async(uri, src_type, True, endpoint):
                    self._send_request(uri, results, src_type)
        ckpts.pop(-1)
        return

    def _handle_folder_result(self, res, results, new_entries):
        folders, files = [], []
        for entry in res["entries"]:
            obj = {"type": entry["type"], "id": entry["id"]}
            if entry["type"] == "folder":
                obj["has_collaborations"] = entry.get("has_collaborations")
                if self.config["collect_folder"]:
                    folders.append(entry)
            elif self.config["collect_file"]:
                files.append(entry)
            new_entries.append(obj)

        for objs in (folders, files):
            if objs:
                results.append(_JObject({"entries": objs},
                                        self.config["url"],
                                        "folder", self._loader))

    def _handle_folder(self, uri, ckpts, results, new_entries):
        err, res = self._send_request(uri, results, "folder", False, False)
        if err:
            if err in (1, 401):
                return False

            ckpts.pop(0)
            return True

        self._handle_folder_result(res, results, new_entries)
        total, offset = res["total_count"], res["offset"]
        if res["entries"] and total > offset + len(res["entries"]):
            ckpts[-1]["offset"] = offset + res["limit"]
            del res
            return False
        else:
            if self.config["collect_folder"]:
                self._folder_count += 1
            if (self.config["collect_collaboration"] and
                    ckpts[-1].get("has_collaborations")):
                uri = self._get_uri(ckpts[-1], "collaborations")
                self._send_request(uri, results, "folderCollaboration")
            ckpts.pop(-1)
            ckpts.extend(new_entries)
            del new_entries[:]
            return True

    def _shutdown(self):
        return super(BoxFolder, self)._shutdown() or self._stopped

    def _run_job_async(self, uri, src_type, verify_entries, endpoint="files"):
        if not self.config["use_thread_pool"]:
            return False

        new_config = {}
        new_config.update(self.config)
        new_config["name"] = new_config["name"].replace("folders", endpoint)
        task = BoxFile(uri, src_type, verify_entries, new_config)
        job = jf.BoxCollectionJob(1000, new_config, task.collect_data)
        try:
            self._loader.run_io_jobs((job,), block=False)
        except Queue.Full:
            return False
        return True

    def _reach_threshhold(self, start_time, ckpts):
        if (self._folder_count + self._file_count >= self.count_threshhold or
                time.time() - start_time >= self.time_threshhold):
            return True
        return False

    def _do_collect(self):
        ckpts = self._get_ckpts()
        results, new_entries, start_time = [], [], time.time()

        self._file_count, self._folder_count = 0, 0
        while ckpts and not self._shutdown():
            uri = self._get_uri(ckpts[-1])
            if ckpts[-1]["type"] == "folder":
                folder_done = self._handle_folder(
                    uri, ckpts, results, new_entries)
                if folder_done:
                    if self._reach_threshhold(start_time, ckpts):
                        break
            else:
                self._handle_file(uri, ckpts, results)

        self._file_total_count += self._file_count
        self._folder_total_count += self._folder_count
        _LOGGER.info("Collect %d folders, %d files in %f seconds. For now, "
                     "%d folders, %d files have been collected.",
                     self._folder_count, self._file_count,
                     time.time() - start_time, self._folder_total_count,
                     self._file_total_count)

        if self._shutdown():
            # If splunkd is shutting down, don't flush anything to stdout
            del results[:]
            return True, results
        elif ckpts:
            # We are in the middle of collecting, reach threshholds
            self._save_state(ckpts)
            return False, results
        else:
            _LOGGER.info("All directories and files have been collected.")
            self._save_state(None)
            return True, results
