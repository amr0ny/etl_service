import logging
import os


class Logger(logging.Logger):
    def __init__(self, logs_path: str, logging_level: str):
        super().__init__(__name__)

        logs_dir = os.path.dirname(logs_path)
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        if not os.path.exists(logs_path):
            with open(logs_path, 'w'):
                pass

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh = logging.FileHandler(logs_path)
        fh.setLevel(logging_level)
        fh.setFormatter(formatter)
        self.addHandler(fh)