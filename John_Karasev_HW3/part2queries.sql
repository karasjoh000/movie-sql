SELECT avg(budget) FROM movie;

SELECT title, company
FROM
  (
    SELECT title, company, iso_3166_1
    FROM movie
      INNER JOIN movie_production_countries ON movie.id=movie_production_countries.movie_id
      INNER JOIN movie_production_companies ON movie_production_companies.movie_id=movie_production_countries.movie_id
      INNER JOIN production_companies company ON movie_production_companies.company_id = company.id
  ) AS full
WHERE full.iso_3166_1='US';

SELECT title, revenue
FROM movie
ORDER BY revenue DESC LIMIT 5;

SELECT mys.title, genre.name
FROM
  (
      SELECT title, g2.name, g.movie_id
      FROM movie
        INNER JOIN movie_genre g ON movie.id = g.movie_id
        INNER JOIN genre g2 ON g.genre_id = g2.id
      WHERE g2.name="Science Fiction"
  ) AS sci
  INNER JOIN
    (
      SELECT title, g2.name, g.movie_id
      FROM movie
        INNER JOIN movie_genre g ON movie.id = g.movie_id
        INNER JOIN genre g2 ON g.genre_id = g2.id
      WHERE g2.name = "Mystery"
    ) AS mys
  ON sci.movie_id=mys.movie_id
  INNER JOIN movie_genre ON movie_genre.movie_id=mys.movie_id
  INNER JOIN genre ON genre.id=movie_genre.genre_id;

SELECT title, popularity
FROM movie
WHERE popularity > (SELECT avg(popularity) FROM movie);

SELECT title, popularity
FROM movie
ORDER BY popularity DESC LIMIT 5;