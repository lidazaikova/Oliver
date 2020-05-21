from socket import error as SocketError
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
import urllib
from joblib import Parallel, delayed
import multiprocessing
import re
import pymysql
import requests
import errno





def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def get_chapter(index):
    print(index)
    if index is None:
        int_index = 0
    else:
        int_index_list = re.findall('[0-9]+', str(index))
        int_index = int(int_index_list[0])
    return '00' + str(index) if int_index < 10 else ('0' + str(index) if int_index < 100 else str(index))


def get_chapter_info(chapter_info_content):
    info = {}

    if len(chapter_info_content) > 0:
        matched = re.match('\((\d+)+.(\d+)+\)', chapter_info_content)
        if matched:
            info['verse'] = int(matched.group(1))
            info['number'] = int(matched.group(2))

    return info


def create_text_mapper():
    data_to_insert = []
    try:
        url_content = requests.get(get_url)
        parsed_text = str(url_content.text).encode("utf-16")#.decode("utf-16")
        soup = BeautifulSoup(parsed_text, "html.parser")
        text_select_el = soup.find('select', id='text_id')
        text_option_elements = text_select_el.findAll('option')

        for text_option_el in text_option_elements:
            data_to_insert.append([text_option_el.text.lower(), text_option_el['value']])
            pass

        cursor.executemany("""INSERT INTO oliver_text_mapping (name, value) VALUES (%s, %s)""", data_to_insert)
        db.commit()
    except SocketError as e:
        print('error: ' + str(e))
        if e.errno != errno.ECONNRESET:
            raise
        pass


def create_chapter_mapper():
    cursor.execute("""TRUNCATE oliver_chapter_mapping""")
    cursor.execute("""SELECT name, value, id FROM oliver_text_mapping WHERE 1""")
    rows = cursor.fetchall()

    for row in rows:
        print(row)
        data_to_insert = []
        name = row['name']
        value = row['value']
        text_id = row['id']

        data = {
            'mode': 'printchapters',
            'textid': value
        }

        url_content = requests.post(post_url, data=data)
        parsed_text = str(url_content.text).encode("utf-16")
        soup = BeautifulSoup(parsed_text, "html.parser")
        chapter_select_el = soup.find('select', id='chapter_id')
        chapter_option_elements = chapter_select_el.findAll('option')

        if len(chapter_option_elements) > 0:
            first_chapter_element = chapter_option_elements[0]
            first_chapter_parts = first_chapter_element.text.lower().split(',')

            cursor.execute("""UPDATE oliver_text_mapping SET short_name=%s WHERE value=%s""", (first_chapter_parts[0], value))

            for chapter_option_el in chapter_option_elements:
                chapter_parts = list(map(str.strip, chapter_option_el.text.lower().split(',')))

                data_to_insert.append([
                    text_id,
                    chapter_parts[1],
                    chapter_parts[2] if len(chapter_parts) > 2 else None,
                    chapter_option_el['value'],
                    chapter_option_el.text]
                )

            cursor.executemany(
                """INSERT INTO oliver_chapter_mapping (text_id, volume, chapter, value, chapter_name) VALUES (%s, %s, %s, %s, %s)""",
                data_to_insert)
        else:
            print('NONE', name)

        pass
    db.commit()
    pass


def create_sentence_mapper():
    cursor.execute("""TRUNCATE oliver_sentence_mapping""")
    cursor.execute("""TRUNCATE oliver_sentence_parallel""")
    db.commit()

    cursor.execute("""SELECT cm.id, cm.volume, cm.chapter, cm.value, tm.short_name, tm.name FROM oliver_chapter_mapping cm LEFT JOIN oliver_text_mapping tm ON tm.id=cm.text_id""")
    rows = cursor.fetchall()

    for row in rows:
        print(row)
        volume = row['volume']
        chapter = row['chapter']
        chapter_id = row['id']
        short_name = row['short_name']
        name = row['name']
        data_to_insert = []
        parallel_data_to_insert = []

        print(name, volume, chapter)

        data = {
            'mode': 'printsentences',
            'chapterid': chapter_id
        }

        url_content = requests.post(post_url, data=data)
        print(url_content.text)
        parsed_text = str(url_content.text).encode("utf-8").decode('utf-8')
        print(parsed_text)
        processed_text = parsed_text.replace('\n', '')
        print(processed_text)
        processed_text = re.sub('<\/?p\d+>', '', processed_text)
        soup = BeautifulSoup(processed_text, "html.parser")
        sentence_els = soup.findAll('div', class_='sentence_div')

        for sentence_el in sentence_els:
            sentence_el = sentence_el
            sentence_id = sentence_el['sentence_id']

            sentence_parallels = soup.findAll('div', class_='sentence_parallels', sentence_id=sentence_id)
            sentence_headline = sentence_el.find('div', class_='sentence_headlines')
            joined_iast = ''.join([str(content) for content in sentence_el.contents]).replace(' ', ' ')

            if sentence_headline:
                joined_iast = joined_iast.replace(str(sentence_headline), '')

            iast = cleanhtml(joined_iast)
            iast_parts = [part.strip() for part in iast.split('/') if len(part) > 0]

            chapter_info = get_chapter_info(iast_parts[len(iast_parts) - 1])

            for sentence_parallel in sentence_parallels:
                links = sentence_parallel.findAll('a')
                for link in links:
                    parsed_link = urllib.parse.parse_qs(link['href'])
                    if parsed_link and parsed_link['PhraseID']:
                        parallel_data_to_insert.append([sentence_id, parsed_link['PhraseID'][0]])
            print(chapter)
            code = str(volume) + '.' + get_chapter(chapter) + '.' + get_chapter(chapter_info['verse']) if short_name == 'mbh' else None

            iast_to_insert = ' / '.join(iast_parts[:-1])

            data_to_insert.append([
                iast_to_insert,
                sentence_headline.text if sentence_headline else None,
                chapter_info['verse'],
                chapter_info['number'],
                chapter_id,
                sentence_id,
                code
            ])


        pass

        cursor.executemany(
            """INSERT INTO oliver_sentence_parallel (sentence_id, sentence_parallel_id) VALUES (%s, %s)""", parallel_data_to_insert)

        cursor.executemany(
            """INSERT INTO oliver_sentence_mapping (iast, headline, verse, number, chapter_id, sentence_id, code) VALUES (%s, %s, %s, %s, %s, %s, %s)""", data_to_insert)
        db.commit()
    pass


def split_parallel_sentences(sentence_id):
    data = {
        'mode': 'printonesentence',
        'sentenceid': sentence_id
    }

    url_content = requests.post(post_url, data=data)
    parsed_text = str(url_content.text).encode("utf-8").decode('utf-8')
    print(parsed_text)
    processed_text = parsed_text.replace('&nbsp;', ' ')\
        .replace('index.php?contents=lemma&IDWord=', '#')\
        .replace('"target', '" target')

    if processed_text == 'no analysis for this sentence':
        processed_text = None

    # print(sentence_id, processed_text)

    return [sentence_id, processed_text]


def split_sentences():
    # 20000
    # cursor.execute("""SELECT sm.sentence_id FROM oliver_sentence_mapping sm ORDER BY sm.sentence_id ASC""")
    execute = True
    fromId = 20000
    offset = 0
    limit = 1000

    while execute:
        cursor.execute("""SELECT sm.sentence_id FROM oliver_sentence_mapping sm 
        LEFT JOIN oliver_chapter_mapping cm ON sm.chapter_id=cm.value 
        LEFT JOIN oliver_text_mapping tm ON tm.id=cm.text_id 
        WHERE sm.sentence_id > %s AND tm.short_name != 'mbh' ORDER BY sm.sentence_id ASC LIMIT %s,%s""", (fromId, offset, limit))
        rows = cursor.fetchall()

        if len(rows) == 0:
            execute = False
            print('CLOSE!')

        num_cores = multiprocessing.cpu_count()

        print('count', len(rows))

        results = Parallel(n_jobs=num_cores)(delayed(split_parallel_sentences)(row[0]) for row in rows)

        for result in results:
            cursor.execute("UPDATE oliver_sentence_mapping SET split_iast=%s WHERE sentence_id=%s", (result[1], result[0]))

        db.commit()

        offset += limit
        print('results', offset, len(results))
    pass
##отладочный метод, его изначально не было
def split_1000_sentenses():
        # 20000
        # cursor.execute("""SELECT sm.sentence_id FROM oliver_sentence_mapping sm ORDER BY sm.sentence_id ASC""")
        execute = True
        fromId = 20000
        offset = 0
        limit = 1000

        cursor.execute("""SELECT sm.sentence_id FROM sanskrit.oliver_sentence_mapping sm 
        LEFT JOIN sanskrit.oliver_chapter_mapping cm ON sm.chapter_id=cm.value 
        LEFT JOIN sanskrit.oliver_text_mapping tm ON tm.id=cm.text_id 
        WHERE sm.sentence_id > %s AND tm.short_name != 'mbh' ORDER BY sm.sentence_id ASC LIMIT %s,%s""",
                       (fromId, offset, limit))
        rows = cursor.fetchall()
        if len(rows) == 0:
           execute = False
           print('CLOSE!')

        num_cores = multiprocessing.cpu_count()

        print('count', len(rows))
        print(rows)
        results = []
        for row in rows:
            results.extend(split_parallel_sentences(row))

        results.remove('invalid sentenceid')
        print(results)

        for result in results:
            print(result)
            if (result != 'invalid sentenceid'):
                cursor.execute("UPDATE oliver_sentence_mapping SET split_iast=%s WHERE sentence_id=%s",
                            (result[1], result[0]))


        db.commit()


        print('results', offset, len(results))

if __name__ == '__main__':

    post_url = 'http://sanskrit-linguistics.org/dcs/ajax-php/ajax-text-handler-wrapper.php'
    get_url = 'http://sanskrit-linguistics.org/dcs/index.php?contents=texte'

    db = pymysql.connect(host='localhost', port=3333,
                         user='root',
                         password='',
                         db='sanskrit',
                         charset='utf8',
                         cursorclass=pymysql.cursors.DictCursor)
    cursor = db.cursor()

    #create_text_mapper()
    #create_chapter_mapper()
    #create_sentence_mapper()

    split_sentences() #<---падает вот на этом методе. все, что выше работает. В методе create_sentence_mapper
    ##идет логгирование выгружаемых запросов, там можно увидеть разметку тоже.

    cursor.close()
    db.close()
