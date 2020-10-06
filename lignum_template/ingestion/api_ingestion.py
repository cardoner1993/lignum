import sys
import threading

from lib.python.services.api.apiExtractor import ApiExtractor


def run_thread_by_arg(arg_vector, func):
    threads = []
    for arg in arg_vector:
        try:
            threads.append(Thread(arg, func))
            print(f"Thread: {arg} created")
        except:
            print("Error: unable to start thread")

    for i in threads:
        i.run()


class Thread(threading.Thread):
    def __init__(self, cname, func):
        threading.Thread.__init__(self)
        self.cname = cname
        self.func = func

    def run(self):
        self.func(self.cname)


def api_ingestion(url):
    ae = ApiExtractor()
    data = ae.get_json_from_api(url)
    file_location = ae.write_json_file(data)
    print(f"{file_location} writed")


if __name__ == "__main__":
    run_thread_by_arg(sys.argv[1:], api_ingestion)
