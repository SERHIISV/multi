# _*_ coding:utf-8 _*_
import csv
import sys
import os
import zipfile
import sqlite3
import threading

from os.path import join, isfile
from os import listdir

from sql_commands import CREATE_TABLE, INSERT


def loader(zipname):
    print '---', zipname
    path = 'data/' + zipname
    conf_path = join(os.path.dirname(os.path.abspath(__file__)), path)
    filename = zipname.strip('.zip')
    with zipfile.ZipFile(conf_path) as f:
        with f.open(filename, 'r') as csv_file:
            content = csv.reader(csv_file, delimiter=',')
            parser(content, zipname)


def parser(content, zipname):
    try:
        conn = sqlite3.connect('sql.db')
        c = conn.cursor()
        c.execute(CREATE_TABLE.format(table_name=zipname.split('.csv.zip')))
        print 'craates %s' % zipname.split('.csv.zip')
    except sqlite3.Error, e:
        print "Error %s:" % e.args[0]

    headers = next(content)
    scalr_meta = headers.index('user:scalr-meta')
    cost_index = headers.index('Cost')

    for raw in content:
        if raw[scalr_meta] and ':' in raw[scalr_meta]:
            object_type = 'env'
            object_id = raw[scalr_meta].split(':')[1]
            cost = raw[cost_index]
            try:
                c.execute(
                    INSERT.format(table_name=zipname.split('.csv.zip')),
                    (object_type, object_id, cost)
                )
            except sqlite3.Error, e:
                print "Error %s:" % e.args
    try:
        conn.commit()
        conn.close()
        print 'wrote to db'
    except sqlite3.Error, e:
        print "Error %s:" % e.args[0]


if __name__ == "__main__":
    files = [f for f in listdir('data') if isfile(join('data', f))]
    for file in files:
        print file
        thread = threading.Thread(target=loader, args=[file])
        thread.start()
        # proc = Process(target=loader, args=(file,))
        # proc.start()
