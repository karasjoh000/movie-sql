import pymysql
import csv
import json


# TODO: CHECK THAT ALL CSV ROWS ARE IN DATABASE!!!!


# create the empty tables
def create_tables(cur):
    # stores all columns that where atomic in the csv
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
                'status VARCHAR(50),'
                'tagline TEXT, '
                'title VARCHAR(100), '
                'vote_average FLOAT, '
                'vote_count DOUBLE, '
                'PRIMARY KEY(id))')

    # all columns that where not atomic, where split into two tables.
    # One table was used to store specific information like genres and their id's
    # or keywords and their id's. The other table was used to show how the first
    # table relates to the movie table.

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
                'company_id INT,'
                'PRIMARY KEY(id),'
                'FOREIGN KEY(movie_id) REFERENCES movie(id),'
                'FOREIGN KEY(company_id) REFERENCES production_companies(id))')

    cur.execute('CREATE TABLE production_countries('
                'iso_3166_1 VARCHAR(2), '
                'cname VARCHAR(50), '
                'PRIMARY KEY(iso_3166_1))')

    cur.execute('CREATE TABLE movie_production_countries('
                'id INT AUTO_INCREMENT, '
                'movie_id INT, '
                'iso_3166_1 VARCHAR(2), '
                'PRIMARY KEY(id), '
                'FOREIGN KEY(movie_id) REFERENCES movie(id), '
                'FOREIGN KEY(iso_3166_1) REFERENCES production_countries(iso_3166_1))')

    cur.execute('CREATE TABLE spoken_languages('
                'iso_639_1 VARCHAR(2), '
                'name VARCHAR(50), '
                'PRIMARY KEY(iso_639_1))')

    cur.execute('CREATE TABLE movie_spoken_languages('
                'id INT AUTO_INCREMENT, '
                'movie_id INT, '
                'iso_639_1 VARCHAR(2), '
                'PRIMARY KEY(id), '
                'FOREIGN KEY(movie_id) REFERENCES movie(id), '
                'FOREIGN KEY(iso_639_1) REFERENCES spoken_languages(iso_639_1))')


# format the string values to be stored.
def str_format(text):
    return '"' + str(text.encode('ascii', 'ignore'), 'ascii').replace('"', '\\"') + '"'


# if date is an empty string return 'NULL'
def date_format(date):
    if date == "":
        return 'NULL'
    elif date is None:
        return 'NULL'
    else:
        return "'" + date + "'"


# if value is empty string or None, return 'NULL'
def check_empty(value):
    if value is None:
        return 'NULL'
    elif value == "":
        return 'NULL'
    else:
        return value


# update each time a row is received from the csv.
def update_table(row, cur):
    # update the movies table with it's corresponding values.
    cur.execute(
        'INSERT INTO movie VALUE({0}, {1}, {2}, {3}, {4}, {5}, '
        '{6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14})'.format(
            row['id'], row['budget'], str_format(row['original_title']), str_format(row['original_language']),
            str_format(row['homepage']), str_format(row['overview']), row['popularity'],
            date_format(row['release_date']), row['revenue'], check_empty(row['runtime']), str_format(row['status']),
            str_format(row['tagline']), str_format(row['title']), row['vote_average'], row['vote_count']))

    # for each genre in csv row, update the movie_genre table and genre table.
    for genre in json.loads(row['genres']):
        # update genre table on duplicate to not store genre more than once
        cur.execute('INSERT INTO genre(id, name) '
                    'VALUE({0}, {1}) '
                    'ON DUPLICATE KEY UPDATE id=id'.format(genre['id'], str_format(genre['name'])))
        # update movie_genre with foreign keys to show what genres belong to the movie in the current row.
        cur.execute('INSERT INTO movie_genre(movie_id, genre_id) '
                    'VALUE({0}, {1})'.format(row['id'], genre['id']))

    # for each keyword in csv row, update the movie_keywords and keywords table.
    for keyword in json.loads(row['keywords']):
        # update keywords table on duplicates to not store a keyword more than once
        cur.execute('INSERT INTO keywords(id, keyword)'
                    'VALUE({0}, {1})'
                    'ON DUPLICATE KEY UPDATE id=id'.format(keyword['id'], str_format(keyword['name'])))
        # update movie_genre with foreign keys to show what genres belong to the movie in the current row. .
        cur.execute('INSERT INTO movie_keywords(movie_id, keyword_id) '
                    'VALUE({0}, {1})'.format(row['id'], keyword['id']))

    # for each company in csv row, update the movie_production_companies and production_companies table.
    for company in json.loads(row['production_companies']):
        # update production_companies table on duplicates to not store a company more than once
        cur.execute('INSERT INTO production_companies(id, company)'
                    'VALUES({0}, {1})'
                    'ON DUPLICATE KEY UPDATE id=id'.format(company['id'], str_format(company['name'])))
        # update movie_production_companies with foreign keys to show what companies belong
        # to the movie in the current row.
        cur.execute('INSERT INTO movie_production_companies(movie_id, company_id)'
                    'VALUE({0}, {1})'.format(row['id'], company['id']))

    # for each company in csv row, update the movie_production_countries and production_countries table.
    for country in json.loads(row['production_countries']):
        # update production_countries table on duplicates to not store a country more than once
        cur.execute('INSERT INTO production_countries(iso_3166_1, cname) '
                    'VALUE({0}, {1})'
                    'ON DUPLICATE KEY UPDATE iso_3166_1=iso_3166_1'.format(str_format(country['iso_3166_1']),
                                                                           str_format(country['name'])))
        # update movie_production_countries with foreign keys to show what country belong
        # to the movie in the current row.
        cur.execute('INSERT INTO movie_production_countries(movie_id, iso_3166_1)'
                    'VALUE({0}, {1})'.format(row['id'], str_format(country['iso_3166_1'])))

    for language in json.loads(row['spoken_languages']):
        # update spoken_languages table on duplicates to not store a country more than once
        cur.execute('INSERT INTO spoken_languages(iso_639_1, name) '
                    'VALUE({0}, {1})'
                    'ON DUPLICATE KEY UPDATE iso_639_1=iso_639_1'.format(str_format(language['iso_639_1']),
                                                                           str_format(language['name'])))
        # update movie_spoken_languages with foreign keys to show what languages belong
        # to the movie in the current row.
        cur.execute('INSERT INTO movie_spoken_languages(movie_id, iso_639_1)'
                    'VALUE({0}, {1})'.format(row['id'], str_format(language['iso_639_1'])))


def main():
    # connect to the database
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='john', passwd='Jkarasev37', db='movies', )
    cur = conn.cursor()

    # create empty tables
    # create_tables(cur)

    # open csv reader
    reader = csv.DictReader(open('src/tmdb_5000_movies.csv', encoding='utf-8'))

    # for each row in csv, update database
    for row in reader:
        update_table(row, cur)

    # save the changes
    conn.commit()

    conn.close()
    cur.close()


if __name__ == '__main__':
    main()
