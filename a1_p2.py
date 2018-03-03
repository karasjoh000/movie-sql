import pymysql
import sys


def main():
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='john', passwd='Jkarasev37', db='movies', )
    cur = conn.cursor()
    query = {

        '1': 'SELECT avg(budget) FROM movie',

        '2': 'SELECT title, company '
             'FROM'
             '  ('
             '    SELECT title, company, iso_3166_1'
             '    FROM movie'
             '      INNER JOIN movie_production_countries ON movie.id=movie_production_countries.movie_id'
             '      INNER JOIN movie_production_companies '
             '      ON movie_production_companies.movie_id=movie_production_countries.movie_id'
             '      INNER JOIN production_companies company ON movie_production_companies.company_id = company.id'
             '  ) AS info '
             'WHERE info.iso_3166_1=\'US\'',

        '3': 'SELECT title, revenue '
             'FROM movie '
             'ORDER BY revenue DESC LIMIT 5',

        '4': 'SELECT mys.title, genre.name '
             'FROM '
             '  ( '
             '      SELECT title, g2.name, g.movie_id '
             '      FROM movie '
             '        INNER JOIN movie_genre g ON movie.id = g.movie_id '
             '        INNER JOIN genre g2 ON g.genre_id = g2.id '
             '      WHERE g2.name="Science Fiction" '
             '   ) AS sci '
             '  INNER JOIN '
             '    ( '
             '      SELECT title, g2.name, g.movie_id '
             '      FROM movie '
             '        INNER JOIN movie_genre g ON movie.id = g.movie_id '
             '        INNER JOIN genre g2 ON g.genre_id = g2.id '
             '      WHERE g2.name = "Mystery" '
             '    ) AS mys '
             '  ON sci.movie_id=mys.movie_id '
             '  INNER JOIN movie_genre ON movie_genre.movie_id=mys.movie_id '
             '  INNER JOIN genre ON genre.id=movie_genre.genre_id',

        '5': 'SELECT title, popularity '
             'FROM movie '
             'WHERE popularity > (SELECT avg(popularity) FROM movie)'

    }[sys.argv[1]]

    for count in range(0, cur.execute(query)):
            row = list(cur.fetchone())
            for value in row:
                if value == row[-1]:
                    print('"'+str(value)+'"')
                else:
                    print('"'+str(value)+'"', end=',')

    conn.close()
    cur.close()


if __name__ == '__main__':
    main()