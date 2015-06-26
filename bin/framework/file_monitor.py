import os.path as op
import traceback
import logging
import log_files


_LOGGER = logging.getLogger(log_files.ta_frmk)


class FileMonitor(object):

    def __init__(self, callback):
        self._callback = callback

        self.file_mtimes = {
            file_name: None for file_name in self.files()
        }
        for k in self.file_mtimes:
            try:
                self.file_mtimes[k] = op.getmtime(k)
            except OSError:
                _LOGGER.debug("Getmtime for %s, failed: %s",
                              k, traceback.format_exc())

    def check_changes(self):
        file_mtimes = self.file_mtimes
        changed_files = []
        for f, last_mtime in file_mtimes.iteritems():
            try:
                current_mtime = op.getmtime(f)
                if current_mtime != last_mtime:
                    file_mtimes[f] = current_mtime
                    changed_files.append(f)
                    _LOGGER.info("Detect %s has changed", f)
            except OSError:
                pass

        if changed_files:
            if self._callback:
                self._callback(changed_files)
            return True
        return False

    def files(self):
        raise NotImplementedError("Derived class shall implement this")
