#!/usr/bin/python
"""
select trim(words.the_word) tw, trim(pages.page_title) pt
from words, pages, pages_words
where words.word_id=pages_words.word_id
and pages.page_id=pages_words.page_id
order by tw;
"""
import getopt
import glob
import logging
import os
import re
import string
import sys
import nltk
import mariadb

HTML4_DOCTYPE = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd"l>'

# connection parameters
conn_params = {
    "user": "root",
    "password": "fubar",
    "host": "localhost",
    "database": "kmapper3"
}


def get_page_titles(db_conn):
    """
    Check index for page.

    :param db_conn: database connection handle
    :param file_name: input file name
    :return: dictionary
    """
    logging.debug("Read titles")
    cursor = db_conn.cursor()
    sqlstr = "select distinct" \
        " trim(words.the_word) tw," \
        " trim(pages.page_title) pt," \
        " trim(page_file) pf" \
        " from words, pages, pages_words" \
        " where words.word_id=pages_words.word_id" \
        " and pages.page_id=pages_words.page_id order by tw;"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # print content
    row = cursor.fetchone()
    prev_word = ""
    pages = {}
    while row:
        the_word = row[0]
        page_title = row[1]
        page_file = row[2]
        # if the_word != prev_word:
        #     print(f"{the_word} :")
        prev_word = the_word
        # print(f"\t{page_title}\n\t\t{page_file}")
        if the_word not in pages:
            pages[the_word] = [(page_title, page_file)]
        else:
            pages[the_word].append((page_title, page_file))
        row = cursor.fetchone()
    cursor.close()
    return pages


def print_word_pages(pages):
    """
    Read page info.
    """
    for the_word in pages:
        print(f"{the_word} :")
        for page_info in pages[the_word]:
            page_title = page_info[0]
            page_file = page_info[1]
        print(f"\t{page_title}\n\t\t{page_file}")
    return 0


def write_word_pages(pages, output_path, link_path, index_path="."):
    """
    Read page info.
    """
    idx_file = "index.html"
    logging.info("Write index %s", idx_file)
    idxf = open(idx_file, "w", encoding="utf-8")
    idxf.write(f"{HTML4_DOCTYPE}\n")
    idxf.write("<html>\n<head>\n<title>Index</title>\n</head>\n<body>\n")
    idxf.write("<h2>Index</h2>\n")
    logging.debug("Write titles for files to '%s'", output_path)
    prev_idx = ""
    widx = None
    for the_word in pages:
        this_idx = the_word[0].upper()
        if  this_idx != prev_idx:
            if widx is not None:
                widx.write("</body></html>\n")
                widx.close()
            idxf.write(f"<h3>{this_idx}</h3>")
            widx_file = output_path + f"/index_{this_idx}.html"
            logging.info("Index for '%s' : %s", this_idx, widx_file)
            widx = open(widx_file, "w", encoding="utf-8")
            widx.write(f"{HTML4_DOCTYPE}\n")
            widx.write(f"<html>\n<head>\n<title>Index {this_idx}</title>\n</head>\n<body>\n")
            widx.write(f'<a href="{widx_file}">{this_idx}</a><br/>\n')
        prev_idx = this_idx
        output_file = output_path + f"/{the_word}.html"
        widx.write(f'<a href="./{the_word}.html" target="pages">{the_word}</a><br/>\n')
        idxf.write(f'<a href="{output_file}">{the_word}</a><br/>\n')
        logging.info("Write file %s", output_file)
        outf = open(output_file, "w", encoding="utf-8")
        outf.write(f"{HTML4_DOCTYPE}\n")
        outf.write("<html>\n<head>\n<title>{the_word}</title>\n</head>\n<body>\n")
        outf.write(f"<h2>{the_word}</h2>")
        for page_info in pages[the_word]:
            page_title = page_info[0]
            page_file = page_info[1]
            link = link_path + f"/{page_file}"
            outf.write(f'<a href="{link}" target="page">{page_title}</a><br/>\n')
        outf.write("</body></html>\n")
        outf.write("\n")
        outf.close()
    logging.info(f"Wrote {len(pages)} files")
    widx.write("</body></html>\n")
    widx.close()
    idxf.write("</body></html>\n")
    idxf.close()
    return 0


def run_build(db_conn, idx_actn, remainder):
    """
    Run the thing.

    :param db_conn: database connection handle
    :param idx_actn: parameters from command line
    :param remainder: list of arguments, used for file name
    :return: zero
    """
    logging.debug("Run %s", idx_actn)
    if idx_actn["lst_titls"]:
        pages = get_page_titles(db_conn)
        r_val = print_word_pages(pages)
    elif idx_actn["write_pages"]:
        pages = get_page_titles(db_conn)
        r_val = write_word_pages(pages, "./builder", "..")
    else:
        logging.error("Nothing to do")
        return 1
    return r_val


def usage_short(f_exe, h_msg):
    """
    Short help message.

    :param f_exe: executable file name
    """
    print(f"{h_msg}, for more information:")
    print(f"\t{os.path.basename(f_exe)} -h|--help")


def usage(f_exe):
    """
    Help message.

    :param f_exe: executable file name
    """
    f_exe = os.path.basename(f_exe)
    print("Usage:")
    print(f"\t{f_exe} --list [--min=<MIN>] [--max=<MAX>]")


# pylint: disable-next=too-many-branches
def main():
    """
    Start here.
    """
    log_level = logging.WARNING
    # Establish database connection
    db_conn = mariadb.connect(**conn_params)
    try:
        options, remainder = getopt.gnu_getopt(
            sys.argv[1:],
            "hlid:cfpu:",
            ["path=", "class=", "url=", "min=", "max=", "find=", "title",
             "write", "stats", "info", "debug"]
        )
    except getopt.GetoptError as opt_err:
        usage_short(sys.argv[0], str(opt_err))
        return 1
    idx_actn = {"lst_titls": False,
                "write_pages": False,}
    for opt, arg in options:
        if opt == '-h':
            usage(sys.argv[0])
            sys.exit(1)
        elif opt in ("-l", "--title"):
            idx_actn["lst_titls"] = True
        elif opt in ("-l", "--write"):
            idx_actn["write_pages"] = True
        elif opt in ("-i", "--info"):
            log_level = logging.INFO
        elif opt in ("-d", "--debug"):
            log_level = logging.DEBUG
        else:
            usage_short(sys.argv[0], f"Unused option {opt} ({arg})")
            return 1
    logging.basicConfig(level=log_level)
    # Establish database connection
    db_conn = mariadb.connect(**conn_params)
    # Run this thing
    r_val = run_build(db_conn, idx_actn, remainder)
    # Close database
    db_conn.close()
    return 0


if __name__ == "__main__":
    # Check if file was specified
    if len(sys.argv) <= 1:
        usage_short(sys.argv[0], "Incorrect paramaters")
        sys.exit(1)
    M_RV = main()
    sys.exit(M_RV)
