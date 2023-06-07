#!/usr/bin/python
"""
Index downloaded HTML files.
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
from bs4 import BeautifulSoup
from iso639 import languages
from langdetect import detect, DetectorFactory, lang_detect_exception
from nltk.corpus import stopwords

# connection parameters
conn_params= {
    "user" : "root",
    "password" : "fubar",
    "host" : "localhost",
    "database" : "kmapper3"
}

word_senses = {
    "CC" : "coordinating conjunction",
    "CD" : "cardinal digit",
    "DT" : "determiner",
    "EX" : "existential there (like: 'there is' … think of it like 'there exists')",
    "FW" : "foreign word",
    "IN" : "preposition/subordinating conjunction",
    "JJ" : "adjective – ‘big’",
    "JJR" : "adjective, comparative – ‘bigger’",
    "JJS" : "adjective, superlative – ‘biggest’",
    "LS" : "list marker 1)",
    "MD" : "modal – could, will",
    "NN" : "noun, singular ‘- desk’",
    "NNS" : "noun plural – ‘desks’",
    "NNP" : "proper noun, singular – ‘Harrison’",
    "NNPS" : "proper noun, plural – ‘Americans’",
    "PDT" : "predeterminer – ‘all the kids’",
    "POS" : "possessive ending parent’s",
    "PRP" : "personal pronoun –  I, he, she",
    "PRP$" : "possessive pronoun – my, his, hers",
    "RB" : "adverb – very, silently,",
    "RBR" : "adverb, comparative – better",
    "RBS" : "adverb, superlative – best",
    "RP" : "particle – give up",
    "TO" : "– : to go ‘to’ the store.",
    "UH" : "interjection – errrrrrrrm",
    "VB" : "verb, base form – take",
    "VBD" : "verb, past tense – took",
    "VBG" : "verb, gerund/present participle – taking",
    "VBN" : "verb, past participle – taken",
    "VBP" : "verb, sing. present, non-3d – take",
    "VBZ" : "verb, 3rd person sing. present – takes",
    "WDT" : "wh-determiner – which",
    "WP" : "wh-pronoun – who, what",
    "WP$" : "possessive wh-pronoun, eg - whose",
    "WRB" : "wh-adverb, eg- where, when",
}


def detect_language(the_data):
    """
    Detect language

    :param the_data: input string
    :return: output stuff
    """
    DetectorFactory.seed = 0
    try:
        out = detect(the_data)
    except lang_detect_exception.LangDetectException:
        logging.warning("No text features extracted")
        return None, None
    out_full = languages.get(alpha2=out).name
    logging.debug("Text is in %s (%s)", out, out_full)
    return out, out_full


def get_word_index(db_conn, the_word):
    """
    Check index for word.

    :param db_conn: database connection handle
    :param the_word: word to be checked
    :return: word index number
    """
    cursor = db_conn.cursor()
    sqlstr = f"SELECT word_id FROM words WHERE the_word='{the_word}';"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    row = cursor.fetchone()
    if row is not None:
        word_id = int(row[0])
    else:
        word_id = 0
    cursor.close()
    logging.debug("Word '%s' ID is %d", the_word, word_id)
    return word_id


def set_word_index(db_conn, the_word):
    """
    Set index for word.

    :param db_conn: database connection handle
    :param the_word: word to be updated
    :return: word index number
    """
    cursor = db_conn.cursor()
    sqlstr = f"INSERT INTO words(the_word) VALUES('{the_word}')" \
        " RETURNING word_id;"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    row = cursor.fetchone()
    word_id = int(row[0])
    logging.debug("New word '%s' ID is %d", the_word, word_id)
    cursor.close()
    # db_conn.commit()
    return word_id


def get_the_word(db_conn, stop_words, wn_lemma, word):
    """
    Lemmatize the word.

    :param db_conn: database connection handle
    :param stop_words: list of stop words
    :param wn_lemma: wordnet lemmatizer handle
    """
    if len(word) > 1 and word not in stop_words and not word.isnumeric():
        the_word = wn_lemma.lemmatize(word)
        word_id = get_word_index(db_conn, the_word)
        logging.debug("Word %s lemma : %s", word, the_word)
        if word_id == 0:
            word_id = set_word_index(db_conn, the_word)
        return the_word, nltk.corpus.wordnet.synsets(word), word_id
    return '', None, 0


def set_page_word_index(db_conn, word_id, page_id, word_pos):
    """
    Update index for words in pages.

    :param db_conn: database connection handle
    :param word_id: word index number
    :param page_id: page index number
    :param word_pos: offset in document
    """
    cursor = db_conn.cursor()
    sqlstr = "INSERT INTO pages_words(word_id, page_id, word_pos)" \
        f" VALUES({word_id}, {page_id}, {word_pos});"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    db_conn.commit()


def get_the_words(the_data):
    """
    Change all punctuation characters to space character.

    :param the_data: input string
    :return: list of words
    """
    # pylint: disable-next=fixme
    # TODO also try
    # the_words = nltk.word_tokenize(the_data)
    puncs = str(string.punctuation)
    translator = str.maketrans(puncs, ' '*len(puncs))
    the_words = the_data.lower().translate(translator).split(" ")
    return the_words


def check_the_nouns(words):
    """
    Extract nouns from words.

    :param words: list of words
    :return: list of nouns
`   """
    the_nouns = []
    tagged = nltk.pos_tag(words)
    for tag in tagged:
        text = tag[0]
        val = tag[1]
        try:
            ws_val = word_senses[val]
        except KeyError:
            logging.warning("No word sense for %s", val)
            continue
        #
        # if(val == 'NN' or val == 'NNS' or val == 'NNPS' or val == 'NNP'):
        if val in ('NN', 'NNS', 'NNPS', 'NNP'):
            logging.debug("%s [%s] is a noun", text, val)
            the_nouns.append(text)
        else:
            logging.info("%s [%s] is not a noun : %s", text, val, ws_val)
    return the_nouns


def get_page_index(db_conn, file_name):
    """
    Check index for page.

    :param db_conn: database connection handle
    :param file_name: input file name
    :return: page index number
    """
    cursor = db_conn.cursor()
    sqlstr = f"SELECT page_id FROM pages WHERE page_file='{file_name}';"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # print content
    row = cursor.fetchone()
    try:
        pg_idx = int(row[0])
        logging.info("File %s indexed as %d", file_name, pg_idx)
    except TypeError:
        pg_idx = -1
        logging.info("File %s not indexed yet", file_name)
    cursor.close()
    # db_conn.commit()
    return pg_idx


def index_page(db_conn, file_name):
    """
    Populate table with some data.

    :param db_conn: database connection handle
    :param file_name: input file name
    """
    cursor = db_conn.cursor()
    cursor.execute(
        "INSERT INTO pages(page_file, page_title, page_url)"
        " VALUES (?,?,?) RETURNING page_id;",
        (file_name, "THE TITLE", "")
    )
    # print content
    row = cursor.fetchone()
    page_id = int(row[0])
    cursor.close()
    db_conn.commit()
    logging.info("File %s indexed as %d", file_name, page_id)
    return page_id


# pylint: disable-next=too-many-locals
def read_book_data(db_conn, sp_data, stop_words, wn_lemma, bk_idx):
    """
    Read body text extracted from web page
    """
    keywords = {}
    all_data = ""
    logging.debug("DATA[%s]", sp_data)
    # Remove HTML tags
    the_data = re.sub('<[^<]+?>', '', str(sp_data))
    # Replace single and double quotes with spaces
    the_data = the_data.replace("’", " ").replace('”', " ")
    # logging.debug("DATA[%s]", the_data)
    out, out_full = detect_language(the_data)
    if out is None:
        return keywords, all_data
    # Check that text is in English
    if out != "en":
        logging.error("%s language (%s) not supported", out_full, out)
        return keywords, all_data
    the_words = get_the_words(the_data)
    chk_words = []
    word_count = 0
    for the_word in the_words:
        # Removes unwanted characters
        # pylint: disable-next=fixme
        # TODO find a better way
        word = the_word.replace('“', '')\
            .replace('—', '')\
            .replace('‘', '')\
            .replace('…', '')
        word_count += 1
        all_data += word
        all_data += " "
        wrd, syns, word_id = get_the_word(
            db_conn, stop_words, wn_lemma, word
        )
        if wrd:
            logging.debug("S> %s [%s] %s", word, wrd, syns)
            set_page_word_index(db_conn, word_id, bk_idx, word_count)
            chk_words.append(wrd)
        else:
            logging.debug("S> %s", word)
    the_nouns = check_the_nouns(chk_words)
    for noun in the_nouns:
        if noun not in keywords:
            keywords[noun] = 1
            logging.debug("Add : %s", noun)
        else:
            keywords[noun] += 1
    return keywords, all_data


def clear_pages_words(db_conn, bk_idx):
    """
    Clear indexed words.

    :param db_conn: database connection handle
    :bk_idx: page index number
    """
    cursor = db_conn.cursor()
    sqlstr = f"DELETE FROM pages_words WHERE page_id={bk_idx};"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    db_conn.commit()


# pylint: disable-next=too-many-arguments,too-many-locals
def read_book(db_conn, stop_words, wn_lemma, file_name, bk_idx,
              bk_xpath, bk_xclass):
    """
    Read the document.

    :param db_conn: database connection handle
    :param db_conn
    :stop_words: list of stop words
    :wn_lemma: WordNet lemmatizer handle
    :file_name: input file name
    :bk_idx: page index number
    :bk_xpath: Xpath string
    :bk_xclass: class associated with Xpath
    """
    logging.debug("Process file %s [%s/%s]", file_name, bk_xpath, bk_xclass)
    clear_pages_words(db_conn, bk_idx)
    with open(file_name, "r", encoding="utf-8") as f_bk:
        html_data = f_bk.read()
    soup = BeautifulSoup(html_data, 'html.parser')
    # Get title
    bk_title = ""
    soup1 = soup.find_all("title", class_=None)
    for sp_data in soup1:
        bk_title = re.sub('<[^<]+?>', '', str(sp_data))
    logging.info("Title is %s", bk_title)
    # Get the rest
    # soup2 = soup.find_all("p", class_=None)
    soup2 = soup.find_all(bk_xpath, class_=bk_xclass)
    keywords = {}
    all_data = ""
    for sp_data in soup2:
        keywords, all_data = read_book_data(
            db_conn, sp_data, stop_words, wn_lemma, bk_idx
        )
    logging.info("Found %d keywords", len(keywords))
    for keyword in keywords:
        logging.debug("%3d : %s", keywords[keyword], keyword)
    if not all_data:
        logging.warning("No data extracted")
    sqlstr = f"UPDATE pages SET page_title='{bk_title}'," \
        " page_text='{all_data}'" \
        f"WHERE page_id={bk_idx};"
    cursor = db_conn.cursor()
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    db_conn.commit()


def list_words(db_conn, wc_min, wc_max):
    """
    Display index for words.

    :param db_conn: database connection handle
    :param wc_min: minimum word count
    :param wc_max: maximum word count
    :return: zero
    """
    cursor = db_conn.cursor()
    sqlstr = "select words.the_word, count(pages_words.word_pos) as word_count" \
        " from words, pages_words" \
        " where pages_words.word_id = words.word_id" \
        " group by pages_words.word_pos"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # row = cursor.fetchone()
    # while row is not None:
    #     print(*row, sep=' ')
    #     row = cursor.fetchone()
    for word, w_count in cursor:
        show_it = True
        if wc_min and w_count <= wc_min:
            show_it = False
        if wc_max and w_count >= wc_max:
            show_it = False
        if show_it:
            print(f"{int(w_count):3d} : {word}")
    cursor.close()
    return 0


def find_words(db_conn, f_word):
    """
    Display index for words.

    :param db_conn: database connection handle
    :param f_word: word to search for
    :return: zero
    """
    cursor = db_conn.cursor()
    sqlstr = "select words.the_word, " \
        " pages.page_title," \
        " pages.page_url" \
        " from words, pages, pages_words" \
        " where pages_words.word_id = words.word_id" \
        " and pages_words.page_id = pages.page_id" \
        f" and words.the_word='{f_word}'"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # row = cursor.fetchone()
    # while row is not None:
    #     print(*row, sep=' ')
    #     row = cursor.fetchone()
    for word, title, url in cursor:
        print(f"{word} : {title} [{url}]")
    cursor.close()
    return 0


def read_files(db_conn, file_name, bk_xpath, bk_xclass):
    """
    Read downloaded pages

    :param db_conn: database connection handle
    :param file_name: input file name or path
    :param bk_xpath: Xpath to select from body
    :param bk_xclass: class associated with Xpath
    :return: zero
    """
    # Initialize natural language processing
    nltk.download('wordnet')
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('omw-1.4')
    nltk.download('averaged_perceptron_tagger')
    # Get stop words
    stop_words = set(stopwords.words('english'))
    _porter_stemmer = nltk.stem.porter.PorterStemmer()
    wn_lemma = nltk.stem.WordNetLemmatizer()
    if '*' in file_name:
        logging.info("Read path %s", file_name)
        n_files = 0
        for f_name in glob.iglob(file_name, recursive=True):
            if os.path.isdir(f_name):
                continue
            logging.info("Read file %s", f_name)
            bk_idx = get_page_index(db_conn, f_name)
            if bk_idx <= 0:
                bk_idx = index_page(db_conn, f_name)
            read_book(
                db_conn, stop_words, wn_lemma, f_name, bk_idx, bk_xpath,
                bk_xclass
            )
            n_files += 1
        logging.info("Read %d files", n_files)
    elif os.path.isfile(file_name):
        # One file only
        logging.info("Read file %s", file_name)
        bk_idx = get_page_index(db_conn, file_name)
        if bk_idx <= 0:
            bk_idx = index_page(db_conn, file_name)
        read_book(
            db_conn, stop_words, wn_lemma, file_name, bk_idx, bk_xpath,
            bk_xclass
        )
        logging.info("Read file %s", file_name)
    elif os.path.isdir(file_name):
        # All the files
        n_files = 0
        root_dir = file_name + '/**/*.html'
        logging.info("Read directory %s", root_dir)
        for f_name in glob.iglob(root_dir, recursive=True):
            if os.path.isdir(f_name):
                continue
            logging.info("Read file %s", file_name)
            bk_idx = get_page_index(db_conn, f_name)
            if bk_idx <= 0:
                bk_idx = index_page(db_conn, f_name)
            read_book(
                db_conn, stop_words, wn_lemma, f_name, bk_idx, bk_xpath,
                bk_xclass
            )
            n_files += 1
        logging.info("Read %d files", n_files)
    else:
        logging.error("File %s is not valid", file_name)
    return 0


def set_page_title(db_conn, file_name, f_title):
    """
    Set page title.

    :param db_conn: database connection handle
    :param file_name: input file name or path
    :param f_title: page title
    :return: zero
    """
    cursor = db_conn.cursor()
    bk_idx = get_page_index(db_conn, file_name)
    if bk_idx <= 0:
        bk_idx = index_page(db_conn, file_name)
    sqlstr = f"UPDATE pages SET page_title='{f_title}' WHERE page_id={bk_idx};"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    db_conn.commit()
    return 0


def set_page_url(db_conn, file_name, f_url):
    """
    Set page title.

    :param db_conn: database connection handle
    :param file_name: input file name or path
    :param f_url: URL of original page
    :return: zero
    """
    cursor = db_conn.cursor()
    bk_idx = get_page_index(db_conn, file_name)
    if bk_idx <= 0:
        bk_idx = index_page(db_conn, file_name)
    sqlstr = f"UPDATE pages SET page_url='{f_url}' WHERE page_id={bk_idx};"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    db_conn.commit()
    return 0


# pylint: disable-next=too-many-arguments
def run_index(db_conn, wc_min, wc_max, f_word, f_url, bk_xpath, bk_xclass,
              lst_wrds, remainder):
    """
    Run the thing.

    :param db_conn: database connection handle
    :param wc_min: minimum word count
    :param wc_max: maximum word count
    :param f_word: word to search for
    :param f_url: URL of original page
    :param bk_xpath: Xpath to select from body
    :param bk_xclass: class associated with Xpath
    :param lst_wrds: flag to list words
    :param remainder: list of arguments, used for file name
    :return: zero
    """
    if lst_wrds:
        r_val = list_words(db_conn, wc_min, wc_max)
    elif f_word:
        r_val = find_words(db_conn, f_word)
    elif f_url:
        file_name = remainder[0]
        r_val = set_page_url(db_conn, file_name, f_url)
    else:
        # Analyze files
        file_name = remainder[0]
        r_val = read_files(db_conn, file_name, bk_xpath, bk_xclass)
    return r_val


def usage(f_exe):
    """
    Help message.

    :param f_exe: executable file name
    """
    f_exe = os.path.basename(f_exe)
    print("Usage:")
    print(f"\t{f_exe} --list- [--min=<MIN>] [--max=<MAX>]")
    print(f"\t{f_exe} --url=<URL> <FILE>")
    print(f"\t{f_exe} [--path=<XPATH>] [--class=<CLASS>] <FILE|PATH>")
    print("where:")
    print("\t--min=<MIN>        minimimun number of word occurences to list")
    print("\t--max=<MAX>        maximum number of word occurences to list")
    print("\t--url=<URL>        URL that was crawled")
    print("\t--path=<XPATH>     Xpath for selection of text")
    print("\t--class=<CLASS>    used with Xpath")
    print("\t<FILE>             file name")
    print("\t<PATH>             directory name")


def main():
    """
    Start here.
    """
    log_level = logging.WARNING
    # Establish database connection
    db_conn = mariadb.connect(**conn_params)
    options, remainder = getopt.gnu_getopt(
        sys.argv[1:],
        "hlid:cfpu:",
        ["path=","class=","url=","min=","max=","find=","list","info","debug"]
    )
    f_word = None
    wc_min = 0
    wc_max = 0
    lst_wrds = False
    f_url = None
    bk_xpath = "p"
    bk_xclass = None
    for opt, arg in options:
        if opt == '-h':
            usage(sys.argv[0])
            sys.exit(1)
        elif opt in ("-p", "--path"):
            bk_xpath = arg
        elif opt in ("-c", "--class"):
            bk_xclass = arg
        elif opt in ("-f", "--find"):
            f_word = arg
        elif opt in ("-u", "--url"):
            f_url = arg
        elif opt == "--min":
            wc_min = int(arg)
        elif opt == "--max":
            wc_max = int(arg)
        elif opt in ("-l", "--list"):
            lst_wrds = True
        elif opt in ("-i", "--info"):
            log_level = logging.INFO
        elif opt in ("-d", "--debug"):
            log_level = logging.DEBUG
        else:
            print("Unused option {opt} ({arg})")
    logging.basicConfig(level=log_level)
    r_val = run_index(db_conn, wc_min, wc_max, f_word, f_url, bk_xpath,
                      bk_xclass, lst_wrds, remainder)
    # Close database
    db_conn.close()
    return r_val


if __name__ == "__main__":
    # Check if file was specified
    if len(sys.argv) <= 1:
        usage(sys.argv[0])
    M_RV = main()
    sys.exit(M_RV)
