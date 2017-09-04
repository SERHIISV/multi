# _*_ coding:utf-8 _*_
import csv
import sys
import os
import zipfile
import sqlite3

from Queue import Queue
from threading import Thread

from os.path import join, isfile
from os import listdir

from sql_commands import CREATE_TABLE, INSERT


class Parser:

    def __init__(self, file):
        self.file = file

    def loader(self):
        path = 'data/' + self.file
        conf_path = join(os.path.dirname(os.path.abspath(__file__)), path)
        filename = self.file.strip('.zip')
        with zipfile.ZipFile(conf_path) as f:
            with f.open(filename, 'r') as csv_file:
                content = csv.reader(csv_file, delimiter=',')
                self.db_worker(content)

    def db_worker(self, content):
        try:
            self.conn = sqlite3.connect('sql.db', timeout=10)
            self.c = self.conn.cursor()
            self.c.execute(CREATE_TABLE.format(table_name=self.file.split('.csv.zip')))
            print 'craates %s' % self.file.split('.csv.zip')
        except sqlite3.Error, e:
            print "Error %s:" % e.args[0]

        self.parse(content)

        try:
            self.conn.commit()
            self.conn.close()
            print 'wrote to db'
        except sqlite3.Error, e:
            print "Error %s:" % e.args[0]

    def parse(self, content):
        headers = next(content)
        scalr_meta = headers.index('user:scalr-meta')
        cost_index = headers.index('Cost')

        for raw in content:
            if raw[scalr_meta] and ':' in raw[scalr_meta]:
                object_type = 'env'
                object_id = raw[scalr_meta].split(':')[1]
                cost = raw[cost_index]
                try:
                    self.c.execute(
                        INSERT.format(table_name=self.file.split('.csv.zip')),
                        (object_type, object_id, cost)
                    )
                except sqlite3.Error, e:
                    print "Error %s:" % e.args


class Worker(Thread):

    def __init__(self, tasks):
        super(Worker, self).__init__()
        self.tasks = tasks
        self.daemon = True

    def run(self):
        while True:
            file = self.tasks.get()
            try:
                Parser(file).loader()
            finally:
                self.tasks.task_done()


if __name__ == "__main__":
    files = [f for f in listdir('data') if isfile(join('data', f))]

    capacity = 0
    queue = Queue(capacity)

    workers = len(files)
    print 'workers - ', workers

    for _ in range(workers):
        Worker(queue).start()

    for file in files:
        if file != 'README.md':
            queue.put(file)

    queue.join()

    print 'DONE'

