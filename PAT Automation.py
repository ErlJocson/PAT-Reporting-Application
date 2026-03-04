from rich import print
from pathlib import Path
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app import start
from dotenv import load_dotenv

load_dotenv()

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, file_to_watch, callback) -> None:
        self.file_to_watch = os.path.abspath(file_to_watch)
        self.callback = callback
        self.last_run = 0

    def on_modified(self, event) -> None:
        if event.is_directory:
            return
        
        if os.path.abspath(event.src_path) != self.file_to_watch:
            return

        current_time = time.time()

        if current_time - self.last_run < 60:
            return
        
        self.last_run = current_time

        print('Log File Change detected')
        print("Running [bold red]PAT Reporting Automated Script[/bold red] ...")
        self.callback()

def monitor_file(file_path) -> None:
    event_handler = FileChangeHandler(file_path, start)
    observer = Observer()
    directory = os.path.dirname(os.path.abspath(file_path))
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()

    print("Monitoring changes on [underline green]Log.txt[/underline green]")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    print("[bold]PAT Reporting Dashboard Data Management[/bold]")

    log_file_direcotry = os.getenv("LOG_FILE_DIRECTORY")

    monitor_file(Path(log_file_direcotry))
