from __future__ import annotations

import logging
import sys
import time

from watchdog.events import FileReadyEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.utils.file_ready_notifier import FileReadyNotifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ImportWhenReadyHandler(FileSystemEventHandler):
    def on_ready(self, event: FileReadyEvent) -> None:
        logger.info("Importing the now-complete file %s", event.src_path)


path = sys.argv[1] if len(sys.argv) > 1 else "ready"

# Wait until each file's size and modification time have been stable for 2
# seconds before handing it to the import handler.
handler = ImportWhenReadyHandler()
notifier = FileReadyNotifier(handler, settle_interval=2)
observer = Observer()
observer.schedule(notifier, path, recursive=True)

notifier.start()
observer.start()
try:
    while True:
        time.sleep(1)
finally:
    observer.stop()
    notifier.stop()
    observer.join()
    notifier.join()
