from bs4 import BeautifulSoup
import requests

url = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
response = requests.get(url)

#print(response.text.encode('utf-8'))
def soup_scraper():
  soup = BeautifulSoup(response.text, 'lxml')     

#print(soup.prettify().encode('utf-8'))

  soup = soup.find('tbody', {'class': 'lister-list'})

  content = soup.find_all('tr')

  movies = []
  for row in content: 
    cells = row.find_all('td')
    movie_info, rating = cells[1].find('a'), cells[2].find('strong')
    year = cells[1].find('span')
    year = year.text

    rating = rating.text
    movie_name = movie_info.text
    director_actor = movie_info.get('title')

    movies.append({'Cast': director_actor, 
                   'Title': movie_name,
                   'Year': year,
                   'rating': rating})

  print(movies[0])
  return movies
  
soup_scraper()