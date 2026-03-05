from rich import print
from pathlib import Path
import time
import os
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app import start
from app.helper import directory_checker
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

        main_data_dump = Path(os.getenv("MAIN_DATA_DUMP"))
        output_directory = Path(os.getenv("OUTPUT_DATA_DIRECTORY"))

        if not directory_checker(main_data_dump):
            main_data_dump = Path(input("Data Dump Directory: "))

            if not directory_checker(main_data_dump):
                print("User input is [bold red]not valid[/bold red]")
                sys.exit(3000)

        if not directory_checker(output_directory):
            output_directory = Path(input("Output Directory: "))

            if not directory_checker(output_directory):
                print("User input is [bold red]not valid[/bold red]")
                sys.exit(3000)

        self.callback(
            main_data_dump = main_data_dump,
            output_directory = output_directory,
        )

def monitor_file(file_path) -> None:
    event_handler = FileChangeHandler(file_path, start)
    observer = Observer()
    directory = os.path.dirname(os.path.abspath(file_path))
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()

    print("Monitoring changes on [underline green]Log.txt[/underline green]")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    print("[bold]PAT Reporting Dashboard Data Management[/bold]")

    log_file_directory = os.getenv("LOG_FILE_DIRECTORY")

    if log_file_directory and os.path.isfile(log_file_directory):
        
        log_file = Path(log_file_directory)

    else:
        log_file_directory = input("Log file location: ")
        log_file = Path(log_file_directory)

        if not log_file.is_file():
            print("User input is [bold red]not valid[/bold red] -- should be the `log.txt` file location")
            sys.exit(3000)

        print("[bold green]File Exists[/bold green]")

    monitor_file(file_path=log_file)
