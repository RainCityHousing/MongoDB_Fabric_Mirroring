from flask import Flask
from threading import Thread

from mongodb_generic_mirroring import mirror
from utils import log_thread_exceptions


def create_app():
    app = Flask(__name__)
    # Log any unhandled exception from mirror() (e.g. during startup/env
    # validation) through the logging module instead of letting it only be
    # printed to stderr by the default threading excepthook.
    thread_name = Thread(target=log_thread_exceptions, args=(mirror,), kwargs={"name": "mirror"}).start()
    
    @app.route("/")
    def home_page():
        import threading
        for thread in threading.enumerate(): 
            print(thread.name)
        return "The MongoDB Fabric Mirroring Service is running..."
        
    return app

app = create_app()
if __name__ == "__main__":
    app.run()