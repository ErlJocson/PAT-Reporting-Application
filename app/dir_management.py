import os
import sys
import time
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from app.helper import directory_checker

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