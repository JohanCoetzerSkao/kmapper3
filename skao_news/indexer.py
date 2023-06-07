#!/usr/bin/python
import getopt
import glob
import logging
import mariadb
import nltk
import os
import re
import string
import sys
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
    DetectorFactory.seed = 0
    try:
        out = detect(the_data)
    except lang_detect_exception.LangDetectException:
        logging.warning("No text features extracted")
        return None, None
    out_full = languages.get(alpha2=out).name
    logging.info("Text is in %s (%s)", out, out_full)
    return out, out_full


def get_word_index(connection, the_word):
    '''Check index for word.'''
    cursor = connection.cursor()
    sqlstr = "SELECT word_id FROM words WHERE the_word='%s';" % (the_word)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    row = cursor.fetchone()
    if row is not None:
        # print(*row, sep=' ')
        word_id = int(row[0])
    else:
        word_id = 0
    cursor.close()
    logging.debug("Word '%s' ID is %d", the_word, word_id)
    return word_id


def set_word_index(connection, the_word):
    cursor = connection.cursor()
    sqlstr = "INSERT INTO words(the_word) VALUES('%s') RETURNING word_id;" % (the_word)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    row = cursor.fetchone()
    word_id = int(row[0])
    logging.debug("New word '%s' ID is %d", the_word, word_id)
    cursor.close()
    # connection.commit()
    return word_id


def get_the_word(connection, stops, wordnet_lemmatizer, word):
    '''Lemmatize the word.'''
    if len(word) > 1 and word not in stops and not word.isnumeric():
        the_word = wordnet_lemmatizer.lemmatize(word)
        word_id = get_word_index(connection, the_word)
        logging.debug
        if word_id == 0:
            word_id = set_word_index(connection, the_word)
        return the_word, nltk.corpus.wordnet.synsets(word), word_id
    else:
        return '', None, 0


def set_page_word_index(connection, word_id, page_id, word_pos):
    cursor = connection.cursor()
    sqlstr = "INSERT INTO pages_words(word_id, page_id, word_pos) VALUES(%d,%d,%d);" % (word_id, page_id, word_pos)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    # connection.commit()
    return


def get_the_words(the_data):
    '''Change all punctuation characters to space character.'''
    # TODO also try
    # the_words = nltk.word_tokenize(the_data)
    puncs = str(string.punctuation)
    translator = str.maketrans(puncs, ' '*len(puncs))
    the_words = the_data.lower().translate(translator).split(" ")
    return the_words


def check_the_nouns(words):
    '''Check if given word is a noun.'''
    the_nouns = []
    tagged = nltk.pos_tag(words)
    # print(tagged)
    for tag in tagged:
        text = tag[0]
        val = tag[1]
        try:
            ws_val = word_senses[val]
        except KeyError:
            logging.warning("No word sense for %s", val)
            continue
        #
        if(val == 'NN' or val == 'NNS' or val == 'NNPS' or val == 'NNP'):
            logging.debug("%s [%s] is a noun", text, val)
            the_nouns.append(text)
        else:
            logging.debug("%s [%s] is not a noun : %s", text, val, ws_val)
    return the_nouns


def get_page_index(connection, filename):
    '''Check index for page.'''
    cursor = connection.cursor()
    sqlstr = "SELECT page_id FROM pages WHERE page_file='%s';" % (filename)
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


def index_page(connection, filename):
    '''Populate table with some data.'''
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO pages(page_file, page_title, page_url)"
        " VALUES (?,?,?) RETURNING page_id;",
        (filename, "THE TITLE", "")
    )
    # print content
    row = cursor.fetchone()
    print(*row, sep=' ')
    page_id = int(row[0])
    cursor.close()
    # connection.commit()
    return page_id


def read_book(connection, stops, wordnet_lemmatizer, filename, bk_idx,
              bk_xpath, bk_xclass):
    '''Read the document.'''
    logging.debug("Read file %s [%s/%s]", filename, bk_xpath, bk_xclass)
    cursor = connection.cursor()
    sqlstr = "DELETE FROM pages_words WHERE page_id=%d;" % (bk_idx)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    f = open(filename, 'r')
    html_data = f.read()
    f.close()
    soup = BeautifulSoup(html_data, 'html.parser')
    # soup2 = soup.find_all("p", class_=None)
    soup2 = soup.find_all(bk_xpath, class_=bk_xclass)
    keywords = {}
    all_data = ""
    for sp_data in soup2:
        logging.debug("DATA[%s]", sp_data)
        # TODO find a better way
        # the_data = str(sp_data).replace("<p>", "").replace("</p>", "")\
        #     .replace("<em>", "").replace("</em>", "")\
        #     .replace("<u>", "").replace("</u>", "")\
        #     .replace("'", " ").replace('"', " ")
        # the_data = re.sub('<a.*?>|</a> ', '', the_data)
        # the_data = re.sub('<.*>|</.*> ', '', str(sp_data))
        the_data = re.sub('<[^<]+?>', '', str(sp_data))
        the_data = the_data.replace("’", " ").replace('”', " ")
        # logging.debug("DATA[%s]", the_data)
        out, out_full = detect_language(the_data)
        if out is None:
            continue
        # Check that text is in English
        if out != "en":
            logging.error("%s language (%s) not supported", out_full, out)
            cursor.close()
            connection.commit()
            return
        the_words = get_the_words(the_data)
        chk_words = []
        word_count = 0
        for the_word in the_words:
            # TODO find a better way
            word = the_word.replace('“', '')\
                .replace('—', '')\
                .replace('‘', '')\
                .replace('…', '')\
                .replace('…', '')
            word_count += 1
            all_data += word
            all_data += " "
            w, syns, word_id = get_the_word(
                connection, stops, wordnet_lemmatizer, word
            )
            if w:
                logging.debug("S> %s [%s] %s", word, w, syns)
                set_page_word_index(connection, word_id, bk_idx, word_count)
                chk_words.append(w)
            else:
                logging.debug("S> %s" % word)
        the_nouns = check_the_nouns(chk_words)
        for noun in the_nouns:
            if noun not in keywords:
                keywords[noun] = 1
                logging.debug("Add : %s", noun)
            else:
                keywords[noun] += 1
    logging.info("Found %d keywords", len(keywords))
    for keyword in keywords:
        logging.debug("%3d : %s", keywords[keyword], keyword)
    if not all_data:
        logging.warning("No data extracted")
    sqlstr = "UPDATE pages SET page_text='%s' WHERE page_id=%d;" \
        % (all_data, bk_idx)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    connection.commit()
    print(">>>")


def list_words(connection):
    '''Display index for words.'''
    cursor = connection.cursor()
    sqlstr = "select words.the_word, count(pages_words.word_pos) as word_count from words, pages_words where pages_words.word_id = words.word_id group by pages_words.word_pos"
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    # row = cursor.fetchone()
    # while row is not None:
    #     print(*row, sep=' ')
    #     row = cursor.fetchone()
    for word, wc in cursor:
        print("%3d : %s" % (int(wc), word))
    cursor.close()


def usage(exe):
    print("%s [--path=<XPATH>] [--class=<CLASS>]<FILE|PATH>" % exe)
    sys.exit(1)


def main(argv):
    '''Start here.'''
    LOG_LEVEL = logging.WARNING
    # Establish database connection
    connection = mariadb.connect(**conn_params)
    # Initialize natural language processing
    nltk.download('wordnet')
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('omw-1.4')
    nltk.download('averaged_perceptron_tagger')
    # Get stop words
    stops = set(stopwords.words('english'))
    porter_stemmer = nltk.stem.porter.PorterStemmer()
    wordnet_lemmatizer = nltk.stem.WordNetLemmatizer()
    bk_xpath = "p"
    bk_xclass = None
    opts, args = getopt.getopt(sys.argv[1:],"hlid:cp:",["path=","class=","list","info","debug"])
    for opt, arg in opts:
        if opt == '-h':
            usage(sys.argv[0])
        elif opt in ("-p", "--path"):
            bk_xpath = arg
        elif opt in ("-c", "--class"):
            bk_xclass
        elif opt in ("-l", "--list"):
            list_words(connection)
        elif opt in ("-i", "--info"):
            LOG_LEVEL = logging.INFO
        elif opt in ("-d", "--debug"):
            LOG_LEVEL = logging.DEBUG
        else:
            print("Unused option %s (%s)" % (opt, arg))
    logging.basicConfig(level=LOG_LEVEL)
    filename = sys.argv[1]
    if os.path.isfile(filename):
        # One file only
        logging.info("Read file %s", filename)
        bi = get_page_index(connection, filename)
        if bi <= 0:
            bi = index_page(connection, filename)
        read_book(
            connection, stops, wordnet_lemmatizer, filename, bi, bk_xpath,
            bk_xclass
        )
        logging.info("Read file %s", filename)
    elif os.path.isdir(filename):
        # All the files
        n_files = 0
        root_dir = filename + '/**/*.html'
        logging.info("Read directory %s", root_dir)
        for filename in glob.iglob(root_dir, recursive=True):
            logging.info("Read file %s", filename)
            bi = get_page_index(connection, filename)
            if bi <= 0:
                bi = index_page(connection, filename)
            read_book(
                connection, stops, wordnet_lemmatizer, filename, bi, bk_xpath,
                bk_xclass
            )
            n_files += 1
        logging.info("Read %d files", n_files)
    # Close database
    connection.close()


if __name__ == "__main__":
    # Check if file was specified
    if len(sys.argv) <= 1:
        usage(sys.argv[0])
    main(sys.argv[1:])
