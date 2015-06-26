import sys
import os.path as op
import logging
from datetime import (datetime, timedelta)

sys.path.append(op.join(op.dirname(op.abspath(__file__)), "framework"))

import configure as conf
import file_monitor
import utils


#_LOGGER = logging.getLogger("ta_box")


class HipChatConfig(conf.TAConfig):
    """
    Handles box related config, password encryption/decryption
    """

    host_user_passwds = (("url"),
                         ("restapi_base", "access_token"))

    def __init__(self):
        super(HipChatConfig, self).__init__()
        self.default_configs = None

    def get_configs(self):
        _, stanza_configs = super(HipChatConfig, self).get_configs()
        #print stanza_configs
        if not stanza_configs:
            return None, None
        #print self.meta_configs, self.stanza_configs
        return self.meta_configs, self.stanza_configs

    def _get_default_configs(self):
        """
        Get default configuration of this TA
        If default/box.conf doesn't contain the
        default config assign the default configuration
        """

        #print "GETTING DEFAULTS"

        defaults = {}
        appname = utils.get_appname_from_path(op.abspath(__file__))
        conf_mgr = conf.ConfManager(self.meta_configs["server_uri"],
                                    self.meta_configs["session_key"])
        hipchat_conf = conf_mgr.get_conf("nobody", appname, "hipchat")
        #print "appname:",appname,"conf_mgr:",conf_mgr,"hipchat_conf:",hipchat_conf
        # if not box_conf:
            # _LOGGER.error("Failed to get box.conf")
            # raise Exception("Failed to get box.conf")

        for stanza in hipchat_conf:
            #print "stanza: " + stanza
            if stanza["stanza"] in ("hipchat_account", "hipchat_default"):
                defaults.update(stanza)

        booleans =[("use_thread_pool", 1)]
        ints = [("priority", 10),("collection_interval", 30), ("priority", 10), ("record_count", 500), ]
        strs = [("loglevel", "INFO")]

        to_int = lambda v: int(v) if v else None
        all_defaults = ((booleans, utils.is_true), (ints, to_int), (strs, str))

        for default_kvs, convert_func in all_defaults:
            for k, v in default_kvs:
                if k not in defaults:
                    defaults[k] = v
                else:
                    defaults[k] = convert_func(defaults[k])

        defaults["created_after"] = self._get_datetime(
            defaults.get("created_after"))

        self.meta_configs["loglevel"] = defaults["loglevel"]
        defaults["duration"] = defaults["collection_interval"]
        return defaults

    @staticmethod
    def _get_datetime(when):
        year_ago = datetime.utcnow() - timedelta(days=365)
        year_ago = datetime.strftime(year_ago, "%Y-%m-%dT%H:%M:%S")
        if not when:
            return year_ago

        try:
            _ = datetime.strptime(when, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            _LOGGER.warn("{} is in bad format. Expect YYYY-MM-DDThh:mm:ss. "
                         "Use current timestamp.".format(when))
            return year_ago
        return when

    @staticmethod
    def _is_credential_section(section, option):
        encrypted = {
            "box_account": ("client_id", "client_secret",
                            "refresh_token", "access_token"),
            "box_proxy": ("proxy_username", "proxy_password"),
        }
        if section in encrypted and option in encrypted[section]:
            return True
        return False

    def _update_stanza_configs(self, defaults):
        self.decrypt_existing_credentials(defaults, self.host_user_passwds)
        appname = utils.get_appname_from_path(op.abspath(__file__))
        for sc in self.stanza_configs:
            for k in defaults:
                if k not in sc or not sc[k]:
                    sc[k] = defaults[k]
            sc["duration"] = int(sc["duration"])
            sc.update(self.meta_configs)
            sc["appname"] = appname


class HipChatConfMonitor(file_monitor.FileMonitor):
    def __init__(self, callback):
        super(HipChatConfMonitor, self).__init__(callback)

    def files(self):
        app_dir = op.dirname(op.dirname(op.abspath(__file__)))
        return (
            op.join(app_dir, "local", "inputs.conf"),
            op.join(app_dir, "default", "inputs.conf"),
            op.join(app_dir, "local", "hipchat.conf"),
            op.join(app_dir, "default", "hipchat.conf"),
            op.join(app_dir, "bin", "framework", "setting.conf")
        )


def handle_ckpts(client_id, meta_configs):
    import json
    import glob
    import state_store as ss

    cur_dir = op.dirname(op.abspath(__file__))
    modinput = op.join(cur_dir, ".modinput")
    if not op.exists(modinput):
        with open(modinput, "w") as f:
            f.write(json.dumps({"id": client_id}))
        return

    with open(modinput) as f:
        prev_client_id = json.load(f)["id"]

    if prev_client_id != client_id:
        with open(modinput, "w") as f:
            f.write(json.dumps({"id": client_id}))

        _LOGGER.warn("Box account has been changed. Remove previous ckpts.")
        ckpt_files = glob.glob(op.join(meta_configs["checkpoint_dir"], "*"))
        store = ss.StateStore(meta_configs,
                              utils.get_appname_from_path(cur_dir))
        for ckpt in ckpt_files:
            store.delete_state(ckpt)
