import threading
from flask import Flask, redirect, send_file, make_response
from ScrapingUtils.Scraper import run_and_write, DATA_FILENAME

app = Flask(__name__)
# Hooray... globals...
scraperThread = threading.Thread(target=run_and_write)

print("Loaded...")

@app.route("/")
def hello():
    return "hello"

@app.route("/start")
def start():
    print("starting scraper")
    # scraper.start()
    scraperThread.start()
    return "Started scraper thread"

@app.route("/metadata")
def get_data():
    return send_file(DATA_FILENAME, attachment_filename="scraping_output.csv")

@app.route("/sanityCheck")
def return_some_text():
    response = make_response("asdf")
    response.headers["content-type"] = "text/plain"
    return response

if __name__ == "__main__":
    app.run()