#!/usr/bin/python
import os
import sys
import logging
import string
import glob
import nltk
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
import mariadb
from langdetect import detect, DetectorFactory
from iso639 import languages

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
    # out = detect('El gerente de la tienda es un papel clave en este complejo comercial.')
    out = detect(the_data)
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
        #
        if(val == 'NN' or val == 'NNS' or val == 'NNPS' or val == 'NNP'):
            logging.debug("%s [%s] is a noun", text, val)
            the_nouns.append(text)
        else:
            logging.debug("%s [%s] is not a noun : %s", text, val, word_senses[val])
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
    cursor.execute("INSERT INTO pages(page_file, page_title, page_url) VALUES (?,?,?) RETURNING page_id;",
                (filename, "THE TITLE", ""))
    # print content
    row = cursor.fetchone()
    print(*row, sep=' ')
    page_id = int(row[0])
    cursor.close()
    # connection.commit()
    return page_id


def read_book(connection, stops, wordnet_lemmatizer, filename, bk_idx):
    '''Read the document.'''
    logging.debug("Read file %s" % filename)
    cursor = connection.cursor()
    sqlstr = "DELETE FROM pages_words WHERE page_id=%d;" % (bk_idx)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    f = open(filename, 'r')
    html_data = f.read()
    f.close()
    soup = BeautifulSoup(html_data, 'html.parser')
    soup2 = soup.find_all("p", class_=None)
    keywords = {}
    all_data = ""
    for sp_data in soup2:
        the_data = str(sp_data).replace("<p>", "").replace("</p>", "")
        logging.debug(the_data)
        out, out_full = detect_language(the_data)
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
            word = the_word.replace('“', '').replace('—', '').replace('‘', '').replace('…', '').replace('…', '')
            word_count += 1
            all_data += word
            all_data += " "
            w, syns, word_id = get_the_word(connection, stops, wordnet_lemmatizer, word)
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
        for keyword in keywords:
            logging.info("%3d : %s", keywords[keyword], keyword)
    sqlstr = "UPDATE pages SET page_text='%s' WHERE page_id=%d;" % (all_data, bk_idx)
    logging.debug(sqlstr)
    cursor.execute(sqlstr)
    cursor.close()
    connection.commit()
    print(">>>")


def usage(exe):
    print("%s <FILE|PATH>" % exe)
    sys.exit(1)


def main():
    '''Start here.'''
    LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL)
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
    # Check if file was specified
    if len(sys.argv) <= 1:
        usage(sys.argv[0])
    filename = sys.argv[1]
    if os.path.isfile(filename):
        # One file only
        bi = get_page_index(connection, filename)
        if bi <= 0:
            bi = index_page(connection, filename)
        read_book(connection, stops, wordnet_lemmatizer, filename, bi)
    elif os.path.isdir(filename):
        # All the files
        root_dir = "books.toscrape.com"
        for filename in glob.iglob(root_dir + '/**/*.html', recursive=True):
            bi = get_page_index(connection, filename)
            if bi <= 0:
                bi = index_page(connection, filename)
            read_book(connection, stops, wordnet_lemmatizer, filename, bi)
    # Close database
    connection.close()


if __name__ == "__main__":
    main()
