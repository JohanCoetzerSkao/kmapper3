#!/usr/bin/python
import os
import sys
import json
import subprocess
import logging
import mariadb


# connection parameters
conn_params = {
    "user" : "root",
    "password" : "fubar",
    "host" : "localhost",
    "database" : "kmapper3"
}


def get_page_index(connection, filename):
    '''Check index for page.'''
    cursor = connection.cursor()
    if filename[0:2] == "./":
        fname = filename[2:]
    else:
        fname = filename
    sqlstr = "SELECT page_id FROM pages WHERE page_file='%s';" % (fname)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # print content
    row = cursor.fetchone()
    try:
        print(*row, sep=' ')
        pg_idx = int(row[0])
        logging.info("File %s indexed as %d", filename, pg_idx)
    except TypeError:
        pg_idx = -1
        logging.info("File %s not indexed yet", filename)
    cursor.close()
    # connection.commit()
    return pg_idx


def scrape_site(JFILE):
    '''This is our shell command, executed by Popen.'''
    # scrapy crawl spider -o $JFILE
    logging.debug("Write JSON file %s", JFILE)
    scrapy_cmd = "scrapy crawl spider -o %s" % JFILE
    logging.info("run> %s", scrapy_cmd)
    p = subprocess.Popen(scrapy_cmd, stdout=subprocess.PIPE, shell=True)
    p_comm = p.communicate()
    logging.debug(p_comm)


def update_file_index(connection, bk_idx, title):
    logging.debug("Update title '%s' index %d", title, bk_idx)
    cursor = connection.cursor()
    sqlstr = "UPDATE pages SET page_title=QUOTE(\"%s\") WHERE page_id=%d" % (title, bk_idx)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()


def insert_file_index(connection, title, filename, page_url):
    logging.debug("Insert title '%s' file %s URL %s", title, filename, page_url)
    if filename[0:2] == "./":
        fname = filename[2:]
    else:
        fname = filename
    cursor = connection.cursor()
    sqlstr = "INSERT INTO pages (page_title,page_file,page_url) VALUES(QUOTE(\"%s\"),'%s','%s')" \
        % (title, fname, page_url)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()


def usage(exe):
    print("%s <FQDN>" % exe)
    sys.exit(1)


def main():
    '''Start here.'''
    # Check if file was specified
    if len(sys.argv) <= 1:
        usage(sys.argv[0])
    # Establish database connection
    connection = mariadb.connect(**conn_params)
    BDIR='./%s' % sys.argv[1]
    JFILE = "%s.json" % sys.argv[1]
    LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL)
    JFILE='books.toscrape.com.json'
    if os.path.exists(JFILE):
        # os.remove(JFILE)
        logging.debug("Read JSON file %s", JFILE)
    else:
        scrape_site(JFILE)

    try:
        # Open JSON file
        with open(JFILE) as json_file:
            data = json.load(json_file)
            n = 0
            for doc in data:
                n += 1
                title = doc["title"]
                book_url = doc["Book URL"]
                logging.info("Document %d: %s", n, title)
                logging.debug("Download URL : %s", book_url)
                book_path = book_url.split('/')[3:]
                out_file = '/'.join(book_path).replace('-', '_')
                logging.debug("Write file %s", out_file)
                DNAME = os.path.dirname(out_file)
                FNAME = os.path.basename(out_file)
                dir_name = "%s/%s" % (BDIR, DNAME)
                logging.debug("Create directory %s", dir_name)
                try:
                    os.makedirs(dir_name)
                except FileExistsError:
                    logging.debug("Directory %s exists", dir_name)
                file_name = "%s/%s/%s" % (BDIR, DNAME, FNAME)
                logging.debug("Create file %s", file_name)
                bk_idx = get_page_index(connection, file_name)
                if bk_idx > 0:
                    update_file_index(connection, bk_idx, title)
                else:
                    insert_file_index(connection, title, file_name, book_url)
                print

    except FileNotFoundError:
        logging.error("Could not read JSON file %s", JFILE)
    except json.decoder.JSONDecodeError as e:
        logging.error("Could not read file %s : %s", JFILE, str(e))
    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
