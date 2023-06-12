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

from skao_news_config.default import HTML4_DOCTYPE, HTML_DIR, MIN_WORD_COUNT, \
    MAX_WORD_COUNT, MIN_WORD_LEN


# connection parameters
conn_params = {
    "user": "root",
    "password": "fubar",
    "host": "localhost",
    "database": "kmapper3"
}

word_senses = {
    "CC": "coordinating conjunction",
    "CD": "cardinal digit",
    "DT": "determiner",
    "EX": "existential there (like: 'there is' … like 'there exists')",
    "FW": "foreign word",
    "IN": "preposition/subordinating conjunction",
    "JJ": "adjective – ‘big’",
    "JJR": "adjective, comparative – ‘bigger’",
    "JJS": "adjective, superlative – ‘biggest’",
    "LS": "list marker 1)",
    "MD": "modal – could, will",
    "NN": "noun, singular ‘- desk’",
    "NNS": "noun plural – ‘desks’",
    "NNP": "proper noun, singular – ‘Harrison’",
    "NNPS": "proper noun, plural – ‘Americans’",
    "PDT": "predeterminer – ‘all the kids’",
    "POS": "possessive ending parent’s",
    "PRP": "personal pronoun –  I, he, she",
    "PRP$": "possessive pronoun – my, his, hers",
    "RB": "adverb – very, silently,",
    "RBR": "adverb, comparative – better",
    "RBS": "adverb, superlative – best",
    "RP": "particle – give up",
    "TO": "–: to go ‘to’ the store.",
    "UH": "interjection – errrrrrrrm",
    "VB": "verb, base form – take",
    "VBD": "verb, past tense – took",
    "VBG": "verb, gerund/present participle – taking",
    "VBN": "verb, past participle – taken",
    "VBP": "verb, sing. present, non-3d – take",
    "VBZ": "verb, 3rd person sing. present – takes",
    "WDT": "wh-determiner – which",
    "WP": "wh-pronoun – who, what",
    "WP$": "possessive wh-pronoun, eg - whose",
    "WRB": "wh-adverb, eg- where, when",
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
    logging.debug("Get index for '%s'", the_word)
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
    logging.debug("Set index for '%s'", the_word)
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
        logging.debug("Word %s lemma : %s", word, the_word)
        word_id = get_word_index(db_conn, the_word)
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
    logging.debug("Set index for word %d, page %d, offset %d",
                  word_id, page_id, word_pos)
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


def check_the_noun(word):
    return


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
    logging.debug("Get index for file '%s'", file_name)
    cursor = db_conn.cursor()
    sqlstr = "SELECT page_id, page_url" \
        f" FROM pages WHERE page_file='{file_name}';"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # print content
    row = cursor.fetchone()
    try:
        pg_idx = int(row[0])
        pg_url = row[1]
        logging.info("File %s indexed as %d", file_name, pg_idx)
    except TypeError:
        pg_idx = -1
        pg_url = None
        logging.info("File %s not indexed yet", file_name)
    cursor.close()
    # db_conn.commit()
    return pg_idx, pg_url


def index_page(db_conn, file_name):
    """
    Populate table with some data.

    :param db_conn: database connection handle
    :param file_name: input file name
    """
    logging.debug("Set index for file '%s'", file_name)
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


def get_page_words(db_conn, bk_idx, words_list):
    """
    Clear indexed words.

    :param db_conn: database connection handle
    :bk_idx: page index number
    """
    logging.info("Read words for page %d", bk_idx)
    cursor = db_conn.cursor()
    sqlstr = "SELECT the_word FROM words WHERE word_id IN" \
        f" (SELECT DISTINCT word_id FROM pages_words WHERE page_id={bk_idx});"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    row = cursor.fetchone()
    words = []
    while row:
        word = row[0]
        word_file = f"/var/www/html/skao_news/builder/{word}.html"
        # logging.debug("Check word %s file: %s", word, word_file)
        if os.path.isfile(word_file) and word in words_list:
            logging.debug("Add word %s file: %s", word, word_file)
            words.append(row[0])
        row = cursor.fetchone()
    logging.info("Found words: %s", ','.join(words))
    return words


# pylint: disable-next=too-many-locals
def read_page_data(db_conn, sp_data, stop_words, wn_lemma, bk_idx):
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
    chk_words_data = {}
    word_count = 0
    for the_word in the_words:
        # Removes unwanted characters
        # pylint: disable-next=fixme
        # TODO find a better way
        word = the_word.replace('“', '')\
            .replace('—', '')\
            .replace('‘', '')\
            .replace('…', '').strip()
        word_count += 1
        all_data += word
        all_data += " "
        wrd, syns, word_id = get_the_word(
            db_conn, stop_words, wn_lemma, word
        )
        if wrd:
            logging.debug("S> %s [%s] %s", word, wrd, syns)
            chk_words.append(wrd)
            chk_words_data[wrd] = (word_id, word_count)
        else:
            logging.debug("S> %s", word)
    the_nouns = check_the_nouns(chk_words)
    for noun in the_nouns:
        word_id, word_count = chk_words_data[noun]
        if word_id == 0:
            word_id = set_word_index(db_conn, noun)
        logging.info("Index '%s' : %d %d", noun, word_id, word_count)
        set_page_word_index(db_conn, word_id, bk_idx, word_count)
        if noun not in keywords:
            keywords[noun] = 1
            logging.debug("Add : %s", noun)
        else:
            keywords[noun] += 1
    logging.debug("(%s)", all_data)
    return keywords, all_data


# pylint: disable-next=too-many-arguments,too-many-locals
def write_page_file(db_conn, stop_words, wn_lemma, file_name, bk_url, bk_xpath,
                    bk_xclass, bk_idx, words_list):
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
    logging.debug("Read file %s [%s/%s]", file_name, bk_xpath, bk_xclass)
    out_file = file_name.replace("www.skao.int", "pages")
    out_dir = os.path.dirname(out_file)
    if not os.path.exists(out_dir):
        logging.info("Create directory %s", out_dir)
        os.makedirs(out_dir)
    logging.info("Write file %s", out_file)
    f_out = open(out_file, "w", encoding="utf-8")
    f_out.write(f"{HTML4_DOCTYPE}\n")
    f_out.write('<html>\n<head>\n<meta charset="utf-8"/>')
    with open(file_name, "r", encoding="utf-8") as f_bk:
        html_data = f_bk.read()
    soup = BeautifulSoup(html_data, 'html.parser')
    # Get title
    bk_title = ""
    soup1 = soup.find_all("title", class_=None)
    for sp_data in soup1:
        bk_title = re.sub('<[^<]+?>', '', str(sp_data))
    logging.info("Title is %s", bk_title)
    f_out.write(f"<title>{bk_title}</title>\n</head>\n<body>\n")
    if bk_url:
        f_out.write(f'<p><a href="{bk_url}">SKAO news article<a></p><hr/>\n')
    f_out.write(f"<h2>{bk_title}</h2>\n")
    # Get the rest
    # soup2 = soup.find_all("p", class_=None)
    soup2 = soup.find_all(bk_xpath, class_=bk_xclass)
    keywords = {}
    all_data = ""
    for sp_data in soup2:
        logging.debug(sp_data)
        # Remove HTML tags
        all_data += "<p>"
        all_data += re.sub('<[^<]+?>', '', str(sp_data))
        all_data += "</p>\n"
    # Get words to be tagged/linked
    page_words = get_page_words(db_conn, bk_idx, words_list)
    page_links = []
    for word in page_words:
        if word not in page_links:
            page_links.append(word)
            word_link = f' id ="{word}"'
        link = f'<a{word_link} href ="/skao_news/builder/{word}.html" target="pages">{word}</a>'
        word_upper = word.upper()
        link_upper = f'<a{word_link} href ="/skao_news/builder/{word}.html" target="pages">{word_upper}</a>'
        word_name = word[0].upper() + word[1:]
        link_name = f'<a{word_link} href ="/skao_news/builder/{word}.html" target="pages">{word_name}</a>'
        all_data = all_data.replace(word, link)
        all_data = all_data.replace(word_upper, link_upper)
        all_data = all_data.replace(word_name, link_name)
    f_out.write(f"{str(all_data)}\n<hr/>\n")
    for word in sorted(page_words):
        link = f"/skao_news/builder/{word}.html"
        f_out.write(f'<a href="{link}" target="pages">{word}</a>&nbsp;\n')
    f_out.write("</body>\n</html>\n")
    f_out.close()
    # print(f"{all_data}")


def write_files(db_conn, file_name, idx_actn):
    """
    Read downloaded pages

    :param db_conn: database connection handle
    :param file_name: input file name or path
    :param bk_xpath: Xpath to select from body
    :param bk_xclass: class associated with Xpath
    :return: zero
    """
    logging.debug("Write files: %s", file_name)
    bk_xpath = idx_actn["bk_xpath"]
    bk_xclass = idx_actn["bk_xclass"]
    wc_min = idx_actn["wc_min"]
    wc_max = idx_actn["wc_max"]
    word_len = idx_actn["word_len"]
    # Initialize natural language processing
    nltk.download('wordnet')
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('omw-1.4')
    nltk.download('averaged_perceptron_tagger')
    # Get stop words
    stop_words = set(stopwords.words('english'))
    logging.info("Loaded %d stop words", len(stop_words))
    logging.debug("STOP: %s", stop_words)
    _porter_stemmer = nltk.stem.porter.PorterStemmer()  # noqa: F841
    wn_lemma = nltk.stem.WordNetLemmatizer()
    words_list = get_list_words(db_conn, wc_min, wc_max, word_len)
    if '*' in file_name:
        logging.info("Write from path %s", file_name)
        n_files = 0
        for f_name in glob.iglob(file_name, recursive=True):
            if os.path.isdir(f_name):
                continue
            logging.info("Read file %s", f_name)
            bk_idx, bk_url = get_page_index(db_conn, f_name)
            if bk_idx <= 0:
                bk_idx = index_page(db_conn, f_name)
            write_page_file(db_conn, stop_words, wn_lemma, f_name, bk_url,
                            bk_xpath, bk_xclass, bk_idx, words_list)
            n_files += 1
        logging.info("Wrote %d files", n_files)
    elif os.path.isfile(file_name):
        # One file only
        logging.info("Write from file %s", file_name)
        bk_idx, bk_url = get_page_index(db_conn, file_name)
        if bk_idx <= 0:
            bk_idx = index_page(db_conn, file_name)
        write_page_file(db_conn, stop_words, wn_lemma, file_name, bk_url,
                        bk_xpath, bk_xclass, bk_idx, words_list)
        logging.info("Processed file %s", file_name)
    elif os.path.isdir(file_name):
        # All the files
        n_files = 0
        root_dir = file_name + '/**/*.html'
        logging.info("Write from directory %s", root_dir)
        for f_name in glob.iglob(root_dir, recursive=True):
            if os.path.isdir(f_name):
                continue
            logging.info("Write from file %s", file_name)
            bk_idx, bk_url = get_page_index(db_conn, f_name)
            if bk_idx <= 0:
                bk_idx = index_page(db_conn, f_name)
            write_page_file(db_conn, stop_words, wn_lemma, f_name, bk_url,
                            bk_xpath, bk_xclass, bk_idx, words_list)
            n_files += 1
        logging.info("Wrote %d files", n_files)
    else:
        logging.error("File name %s is not valid", file_name)
    return 0


def clear_pages_words(db_conn, bk_idx):
    """
    Clear indexed words.

    :param db_conn: database connection handle
    :bk_idx: page index number
    """
    logging.info("Clear index for page %d", bk_idx)
    cursor = db_conn.cursor()
    sqlstr = f"DELETE FROM pages_words WHERE page_id={bk_idx};"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    db_conn.commit()


def get_pages_words(db_conn, bk_idx):
    """
    Clear indexed words.

    :param db_conn: database connection handle
    :bk_idx: page index number
    """
    logging.info("Clear index for page %d", bk_idx)
    cursor = db_conn.cursor()
    sqlstr = f"SELECT the_word FROM pages_words WHERE page_id={bk_idx};"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    db_conn.commit()


def clear_all(db_conn):
    """
    Clear indexes.

    :param db_conn: database connection handle
    :return: zero
    """
    logging.info("Clear all indexes")
    cursor = db_conn.cursor()
    for sqlstr in ["DELETE FROM pages_words;",
                   "DELETE FROM page_words;",
                   "DELETE FROM pages;",
                   "DELETE FROM words;"]:
        logging.debug(sqlstr)
        cursor.execute(sqlstr)
    db_conn.commit()
    return 0


def get_stats(db_conn):
    """
    Statistics of indexes.

    :param db_conn: database connection handle
    :return: zero
    """
    logging.debug("Get table statistics")
    cursor = db_conn.cursor()
    sqlstrs = {"pages": "SELECT count(page_id) FROM pages;",
               "words": "SELECT count(word_id) FROM words;",
               "page_words": "SELECT count(word_pos) FROM page_words;",
               "pages_words": "SELECT count(word_pos) FROM pages_words;"}
    # pylint: disable-next=consider-using-dict-items
    for table_name in sqlstrs:
        sqlstr = sqlstrs[table_name]
        logging.debug(sqlstr)
        cursor.execute(sqlstr)
        row = cursor.fetchone()
        row_count = int(row[0])
        print(f"Table {table_name} has {row_count} rows")
    cursor.close()
    return 0


# pylint: disable-next=too-many-arguments,too-many-locals
def read_page(db_conn, stop_words, wn_lemma, file_name, bk_idx,
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
        keywords, book_data = read_page_data(db_conn, sp_data, stop_words,
                                             wn_lemma, bk_idx)
        all_data += book_data
    logging.info("Found %d keywords", len(keywords))
    # pylint: disable-next=consider-using-dict-items
    for keyword in keywords:
        logging.debug("%3d : %s", keywords[keyword], keyword)
    if all_data:
        logging.debug("TEXT [%s]", all_data)
        sqlstr = f"UPDATE pages SET page_title='{bk_title}'," \
            f" page_text='{all_data}'" \
            f" WHERE page_id={bk_idx};"
        cursor = db_conn.cursor()
        logging.debug(sqlstr)
        cursor.execute(sqlstr)
    else:
        logging.warning("No data extracted")
    cursor.close()
    db_conn.commit()


def get_list_words(db_conn, wc_min, wc_max, word_len):
    """
    Display index for words.

    :param db_conn: database connection handle
    :param wc_min: minimum word count
    :param wc_max: maximum word count
    :return: list of words
    """
    logging.debug("List words: minimum %d, maximum %d", wc_min, wc_max)
    cursor = db_conn.cursor()
    sqlstr = "select words.the_word, count(pages_words.word_pos)" \
        " as word_count" \
        " from words, pages_words" \
        " where pages_words.word_id = words.word_id" \
        " group by pages_words.word_pos"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    words_list = []
    for word, w_count in cursor:
        show_it = True
        if wc_min and w_count <= wc_min:
            show_it = False
        if wc_max and w_count >= wc_max:
            show_it = False
        if len(word) < word_len:
            show_it = False
        if show_it:
            logging.debug("Add word %d : %s", int(w_count), word)
            words_list.append(word)
    cursor.close()
    return words_list


def list_words(db_conn, wc_min, wc_max, word_len):
    """
    Display index for words.

    :param db_conn: database connection handle
    :param wc_min: minimum word count
    :param wc_max: maximum word count
    :return: zero
    """
    logging.debug("List words: minimum %d, maximum %d", wc_min, wc_max)
    cursor = db_conn.cursor()
    sqlstr = "select words.the_word, count(pages_words.word_pos)" \
        " as word_count" \
        " from words, pages_words" \
        " where pages_words.word_id = words.word_id" \
        " group by pages_words.word_pos"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    for word, w_count in cursor:
        show_it = True
        if wc_min and w_count <= wc_min:
            show_it = False
        if wc_max and w_count >= wc_max:
            show_it = False
        if len(word) < word_len:
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
    logging.debug("Find word '%s'", f_word)
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
    logging.debug("Read files: %s", file_name)
    # Initialize natural language processing
    nltk.download('wordnet')
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('omw-1.4')
    nltk.download('averaged_perceptron_tagger')
    # Get stop words
    stop_words = set(stopwords.words('english'))
    logging.info("Loaded %d stop words", len(stop_words))
    logging.debug("STOP: %s", stop_words)
    _porter_stemmer = nltk.stem.porter.PorterStemmer()  # noqa: F841
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
            read_page(db_conn, stop_words, wn_lemma, f_name, bk_idx, bk_xpath,
                      bk_xclass)
            n_files += 1
        logging.info("Read %d files", n_files)
    elif os.path.isfile(file_name):
        # One file only
        logging.info("Read file %s", file_name)
        bk_idx = get_page_index(db_conn, file_name)
        if bk_idx <= 0:
            bk_idx = index_page(db_conn, file_name)
        read_page(db_conn, stop_words, wn_lemma, file_name, bk_idx, bk_xpath,
                  bk_xclass)
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
            read_page(db_conn, stop_words, wn_lemma, f_name, bk_idx, bk_xpath,
                      bk_xclass)
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
    logging.debug("Set title to '%s'", f_title)
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
    logging.debug("Set URL to '%s'", f_url)
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


def run_index(db_conn, idx_actn, remainder):
    """
    Run the thing.

    :param db_conn: database connection handle
    :param idx_actn: parameters from command line
    :param remainder: list of arguments, used for file name
    :return: zero
    """
    logging.debug("Run %s", idx_actn)
    if idx_actn["clr_idx"]:
        r_val = clear_all(db_conn)
    elif idx_actn["lst_wrds"]:
        r_val = list_words(db_conn, idx_actn["wc_min"], idx_actn["wc_max"],
                           idx_actn["word_len"])
    elif idx_actn["lst_stat"]:
        r_val = get_stats(db_conn)
    elif idx_actn["f_word"]:
        r_val = find_words(db_conn, idx_actn["f_word"])
    elif idx_actn["f_url"]:
        file_name = remainder[0]
        r_val = set_page_url(db_conn, file_name, idx_actn["f_url"])
    elif idx_actn["wrt_html"]:
        file_name = remainder[0]
        r_val = write_files(db_conn, file_name, idx_actn)
    else:
        # Analyze files
        try:
            file_name = remainder[0]
        except IndexError:
            logging.error("File name or path not set")
            return 1
        r_val = read_files(db_conn, file_name, idx_actn["bk_xpath"],
                           idx_actn["bk_xclass"])
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
    print(f"\t{f_exe} --url=<URL> <FILE>")
    print(f"\t{f_exe} [--path=<XPATH>] [--class=<CLASS>] <FILE|PATH>")
    print("where:")
    print("\t--min=<MIN>        minimimun number of word occurences to list")
    print("\t--max=<MAX>        maximum number of word occurences to list")
    print("\t--url=<URL>        URL that was crawled")
    print("\t--path=<XPATH>     Xpath for selection of text")
    print("\t--class=<CLASS>    used with Xpath")
    print("\t--clear            delete all indexes")
    print("\t--stats            statistics of indexes")
    print("\t<FILE>             file name")
    print("\t<PATH>             directory name")


# pylint: disable-next=too-many-branches
def main():
    """
    Start here.
    """
    log_level = logging.WARNING
    try:
        options, remainder = getopt.gnu_getopt(
            sys.argv[1:],
            "hlid:cfpu:",
            ["path=", "class=", "url=", "min=", "max=", "find=", "length=",
             "list", "write", "clear", "stats", "info", "debug"]
        )
    except getopt.GetoptError as opt_err:
        usage_short(sys.argv[0], str(opt_err))
        return 1
    idx_actn = {"lst_wrds": False,
                "wrt_html": False,
                "clr_idx": False,
                "lst_stat": False,
                "wc_min": MIN_WORD_COUNT,
                "wc_max": MAX_WORD_COUNT,
                "word_len": MIN_WORD_LEN,
                "f_word": None,
                "f_url": None,
                "bk_xpath": "p",
                "bk_xclass": None,
                }
    for opt, arg in options:
        if opt == '-h':
            usage(sys.argv[0])
            sys.exit(1)
        elif opt in ("-p", "--path"):
            idx_actn["bk_xpath"] = arg
        elif opt in ("-c", "--class"):
            idx_actn["bk_xclass"] = arg
        elif opt in ("-f", "--find"):
            idx_actn["f_word"] = arg
        elif opt in ("-u", "--url"):
            idx_actn["f_url"] = arg
        elif opt == "--min":
            idx_actn["wc_min"] = int(arg)
        elif opt == "--max":
            idx_actn["wc_max"] = int(arg)
        elif opt == "--length":
            idx_actn["word_len"] = int(arg)
        elif opt in ("-l", "--list"):
            idx_actn["lst_wrds"] = True
        elif opt == "--write":
            idx_actn["wrt_html"] = True
        elif opt == "--stats":
            idx_actn["lst_stat"] = True
        elif opt == "--clear":
            idx_actn["clr_idx"] = True
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
    r_val = run_index(db_conn, idx_actn, remainder)
    # Close database
    db_conn.close()
    return r_val


if __name__ == "__main__":
    # Check if file was specified
    if len(sys.argv) <= 1:
        usage_short(sys.argv[0], "Incorrect paramaters")
        sys.exit(1)
    M_RV = main()
    sys.exit(M_RV)
