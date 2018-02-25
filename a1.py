import pymysql
import csv

def createTables():
	conn = pymysql.connect(host='127.0.0.1', port=3306, user='john', passwd='Jkarasev37', db='movies')

	cur = conn.cursor()

	cur.execute("CREATE TABLE genre(id INT, name VARCHAR(30))")
	cur.execute("CREATE TABLE movie(id INT, name VARCHAR(30))")
	cur.execute("CREATE TABLE movie_genre(id INT, movie_id VARCHAR(30), genre_id VARCHAR(30))")

	conn.close()
	cur.close()

def openCSV(path):
	with open(path, 'r') as movies:
		reader = csv.DictReader(movies)
		for row in reader:
			print(row['genres'], row['id'])

def insertEntry(row):
	
openCSV("./tmdb_5000_movies.csv")
