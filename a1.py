import pymysql
import csv
import json


def create_tables(cur):
    cur.execute('CREATE TABLE movie('
                'id INT,'
                'budget INT,'
                'original_title TEXT, '
                'original_language VARCHAR(2), '
                'homepage TEXT, '
                'overview TEXT, '
                'popularity DOUBLE, '
                'release_date DATE, '
                'revenue DOUBLE, '
                'runtime INT, '
                'status VARCHAR(10),'
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
                'id INT,'
                'movie_id INT,'
                'genre_id INT,'
                'PRIMARY KEY(id),'
                'FOREIGN KEY(movie_id) REFERENCES movie(id),'
                'FOREIGN KEY(genre_id) REFERENCES genre(id))')

    cur.execute('CREATE TABLE keywords('
                'id INT, '
                'keyword INT)')

    cur.execute('CREATE TABLE movie_keywords('
                'id INT, '
                'movie_id INT, '
                'keyword_id INT, '
                'PRIMARY KEY(id), '
                'FOREIGN KEY(movie_id) REFERENCES movie(id), '
                'FOREIGN KEY (keyword_id) REFERENCES keywords(id))')

    cur.execute('CREATE TABLE production_companies('
                'id INT, '
                'company VARCHAR(100))')

    cur.execute('CREATE TABLE movie_production_companies('
                'id INT, '
                'movie_id INT,'
                'company INT,'
                'PRIMARY KEY(id),'
                'FOREIGN KEY(movie_id) REFERENCES movie(id),'
                'FOREIGN KEY(company) REFERENCES production_companies(id))')

def open_csv(path):
    movies = open(path, 'r')
    return csv.DictReader(movies)


def update_table(row, cur):
    # update genre table
    for genre in json.loads(row['genres']):
        print('{0}, {1}'.format(str(genre['id']), str(genre['name'])))
        cur.execute('INSERT INTO genre(id, name) '
                    'VALUES({0}, \'{1}\') '
                    'ON DUPLICATE KEY UPDATE id=id'.format(str(genre['id']), str(genre['name'])))

    cur.execute('INSERT INTO movie()')


def main():
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='john', passwd='Jkarasev37', db='movies')
    cur = conn.cursor()

    # create_tables(cur)

    reader = csv.DictReader(open('tmdb_5000_movies.csv'))

    for row in reader:
        update_table(row, cur)
    conn.commit()

    conn.close()
    cur.close()


if __name__ == '__main__':
    main()
