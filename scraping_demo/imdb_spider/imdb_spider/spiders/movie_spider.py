from pathlib import Path
import scrapy

class MovieSpider(scrapy.Spider):
  name = 'moviespider'
  def start_requests(self):
    url = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
    yield scrapy.Request(url=url, callback=self.parse)

  def parse(self, response):
    movie_table = response.css('.lister-list')
    items = []
    for movie in movie_table:
      yield{
        'Name': movie.css('.titleColumn').css('a::text').get(),
        'Year': movie.css('.titleColumn').css('span::text').get(),
        'Rating': movie.css('.ratingColumn').css('strong::text').get()
      }  