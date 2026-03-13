import os
import sys
import time
from rich import print
from pathlib import Path
from watchdog.observers import Observer
from app.dir_management import FileChangeHandler
from dotenv import load_dotenv
from app import start
from warnings import filterwarnings

filterwarnings('ignore')

load_dotenv()

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
