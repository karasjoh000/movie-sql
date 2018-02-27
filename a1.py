import pymysql
import csv
import json
import binascii as bn


def create_tables(cur):
    cur.execute('CREATE TABLE movie('
                'id INT,'
                'budget INT,'
                'original_title TEXT, '
                'original_language VARCHAR(2), '
                'homepage TEXT, '
                'overview TEXT, '
                'popularity DOUBLE, '
                'release_date VARCHAR(20), '
                'revenue DOUBLE, '
                'runtime INT, '
                'status VARCHAR(50),'
                'tagline TEXT, '
                'title VARCHAR(100), '
                'vote_average FLOAT, '
                'vote_count DOUBLE, '
                'PRIMARY KEY(id))')

    cur.execute('CREATE TABLE genre('
                'id INT,'
                'name VARCHAR(50), '
                'PRIMARY KEY(id))')

    cur.execute('CREATE TABLE movie_genre('
                'id INT AUTO_INCREMENT,'
                'movie_id INT,'
                'genre_id INT,'
                'PRIMARY KEY(id),'
                'FOREIGN KEY(movie_id) REFERENCES movie(id),'
                'FOREIGN KEY(genre_id) REFERENCES genre(id))')

    cur.execute('CREATE TABLE keywords('
                'id INT, '
                'keyword VARCHAR(50), '
                'PRIMARY KEY(id))')

    cur.execute('CREATE TABLE movie_keywords('
                'id INT AUTO_INCREMENT, '
                'movie_id INT, '
                'keyword_id INT, '
                'PRIMARY KEY(id), '
                'FOREIGN KEY(movie_id) REFERENCES movie(id), '
                'FOREIGN KEY (keyword_id) REFERENCES keywords(id))')

    cur.execute('CREATE TABLE production_companies('
                'id INT, '
                'company VARCHAR(100), '
                'PRIMARY KEY(id))')

    cur.execute('CREATE TABLE movie_production_companies('
                'id INT AUTO_INCREMENT, '
                'movie_id INT,'
                'company INT,'
                'PRIMARY KEY(id),'
                'FOREIGN KEY(movie_id) REFERENCES movie(id),'
                'FOREIGN KEY(company) REFERENCES production_companies(id))')


def open_csv(path):
    movies = open(path, 'r')
    return csv.DictReader(movies)


def str_format(text):
    return '"' + str(text.encode('ascii', 'ignore'), 'ascii').replace('"', '\\"') + '"'


def check_empty(value):
    if value is None:
        print("Found None")
        return 'NULL'
    elif value == "":
        return 'NULL'
    else:
        return value


def update_table(row, cur):
    cur.execute(
        'INSERT INTO movie VALUE({0}, {1}, {2}, {3}, {4}, {5}, '
        '{6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14})'.format(
            row['id'], row['budget'], str_format(row['original_title']), str_format(row['original_language']),
            str_format(row['homepage']), str_format(row['overview']), row['popularity'],
            str_format(row['release_date']), row['revenue'], check_empty(row['runtime']), str_format(row['status']),
            str_format(row['tagline']), str_format(row['title']), row['vote_average'], row['vote_count']))

    for genre in json.loads(row['genres']):

        cur.execute('INSERT INTO genre(id, name) '
                    'VALUE({0}, {1}) '
                    'ON DUPLICATE KEY UPDATE id=id'.format(genre['id'], str_format(genre['name'])))

        cur.execute('INSERT INTO movie_genre(movie_id, genre_id) '
                    'VALUE({0}, {1})'.format(row['id'], genre['id']))

    for keyword in json.loads(row['keywords']):

        cur.execute('INSERT INTO keywords(id, keyword)'
                    'VALUE({0}, {1})'
                    'ON DUPLICATE KEY UPDATE id=id'.format(keyword['id'], str_format(keyword['name'])))

        cur.execute('INSERT INTO movie_keywords(movie_id, keyword_id) '
                    'VALUE({0}, {1})'.format(row['id'], keyword['id']))

    for company in json.loads(row['production_companies']):

        cur.execute('INSERT INTO production_companies(id, company)'
                    'VALUES({0}, {1})'
                    'ON DUPLICATE KEY UPDATE id=id'.format(company['id'], str_format(company['name'])))

        cur.execute('INSERT INTO movie_production_companies(movie_id, company)'
                    'VALUE({0}, {1})'.format(row['id'], company['id']))


def main():
    print("in main")
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='john', passwd='Jkarasev37', db='movies', )
    cur = conn.cursor()

    conn.commit()

    create_tables(cur)

    reader = csv.DictReader(open('tmdb_5000_movies.csv', encoding='utf-8'))

    for row in reader:
        update_table(row, cur)

    conn.commit()

    conn.close()
    cur.close()


if __name__ == '__main__':
    main()
