from bs4 import BeautifulSoup
from urllib import urlopen
import re
import time
import pymysql

conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock',
                       user='root', password=password, db='mysql',
                       charset='utf8')


def writeToDb(quotes):
    cur = conn.cursor()
    cur.execute('USE ScrapQuotes')
    try:
        for quote, author, author_about, tags in quotes:
            cur.execute("""
                        SELECT auth_name
                        FROM authors
                        WHERE auth_name = %s
                        """, (author,))

            if not cur.fetchone():
                cur.execute("""
                            INSERT INTO authors (auth_name, auth_about)
                            VALUES (%s, %s)
                            """, (author, author_about,))

            cur.execute("""
                        INSERT INTO quotes (quote_text, auth_id, quote_tags)
                        VALUES (
                                %s,
                                (SELECT auth_id
                                 FROM authors
                                 WHERE auth_name = %s),
                                %s
                                )
                        """, (quote, author, ' '.join(tags),))
    except:
        cur.close()
        conn.close()
    cur.close()
    conn.commit()


def getQuotes(page):
    quotes = []
    for quoteBlock in page.findAll('div', {'class': 'quote'}):
        quote = quoteBlock.find('span', {'class': 'text'}).get_text()
        author = quoteBlock.find('small', {'class': 'author'}).get_text()
        author_about = 'http://quotes.toscrape.com' + \
            quoteBlock.find('a', {'href': re.compile('^/author/.*')})['href']
        tags = []
        for tag in quoteBlock.findAll('a', {'class': 'tag'}):
            tags.append(tag.get_text())
        quotes.append((quote, author, author_about, tags))
    return quotes


def getNextPage(page):
    try:
        next_button = page.find('li', {'class': 'next'})
        next_page = next_button.find('a', href=re.compile('^/page/[0-9]+/$'))['href']
    except:
        return None
    next_url = 'http://quotes.toscrape.com' + next_page
    return next_url


def parse(url):
    while True:
        try:
            html = urlopen(url)
            break
        except:
            print "Can't reach " + url
            print "Going to timeout for a minute"
            time.sleep(60)
    bsObj = BeautifulSoup(html)
    quotes = getQuotes(bsObj)
    writeToDb(quotes)
    next_url = getNextPage(bsObj)
    if next_url:
        time.sleep(3)
        parse(next_url)
    else:
        print "Reached the final page"


if __name__ == '__main__':
    start_url = 'http://quotes.toscrape.com'
    try:
        parse(start_url)
    finally:
        conn.close()
