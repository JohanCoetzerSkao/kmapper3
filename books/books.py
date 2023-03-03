#!/usr/bin/python
import sys
import logging
import string
import glob
import nltk
from bs4 import BeautifulSoup
from nltk.corpus import stopwords

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
    "WP$" : "possessive wh-pronoun, eg- whose",
    "WRB" : "wh-adverb, eg- where, when",
}


def get_the_words(the_data):
    '''map punctuation to space'''
    # TODO also try
    # the_words = nltk.word_tokenize(the_data)
    puncs = str(string.punctuation)
    translator = str.maketrans(string.punctuation, ' '*len(puncs))
    the_words = the_data.lower().translate(translator).split(" ")
    return the_words


def get_the_word(word):
    if len(word) > 1 and word not in stops and not word.isnumeric():
        return wordnet_lemmatizer.lemmatize(word), nltk.corpus.wordnet.synsets(word)
    else:
        return '', None


def check_the_nouns(words):
    the_nouns = []
    tagged = nltk.pos_tag(words)
    # print(tagged)
    for tag in tagged:
        text = tag[0]
        val = tag[1]
        # check if it is a noun
        if(val == 'NN' or val == 'NNS' or val == 'NNPS' or val == 'NNP'):
            logging.debug("%s [%s] is a noun", text, val)
            the_nouns.append(text)
        else:
            logging.debug("%s [%s] is not a noun : %s", text, val, word_senses[val])
    return the_nouns


def read_book(filename):
    logging.debug("Read file %s" % filename)
    f = open(filename, 'r')
    html_data = f.read()
    f.close()
    soup = BeautifulSoup(html_data, 'html.parser')
    soup2 = soup.find_all("p", class_=None)
    keywords = {}
    for sp_data in soup2:
        the_data = str(sp_data).replace("<p>", "").replace("</p>", "")
        logging.debug(the_data)
        the_words = get_the_words(the_data)
        chk_words = []
        for word in the_words:
            w, syns = get_the_word(word)
            if w:
                logging.debug("S> %s [%s] %s", word, w, syns)
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
    print(">>>")

nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('omw-1.4')
nltk.download('averaged_perceptron_tagger')

# LOG_LEVEL = logging.INFO
LOG_LEVEL = logging.DEBUG
logging.basicConfig(level=LOG_LEVEL)

stops = set(stopwords.words('english'))
porter_stemmer = nltk.stem.porter.PorterStemmer()
wordnet_lemmatizer = nltk.stem.WordNetLemmatizer()

if len(sys.argv) > 1:
    filename = sys.argv[1]
    read_book(filename)
else:
    root_dir = "books.toscrape.com"
    for filename in glob.iglob(root_dir + '/**/*.html', recursive=True):
        read_book(filename)
