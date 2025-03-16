
import os
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

class KayakSpider(scrapy.Spider):
    name = 'booking_spider'
    allowed_domains = ['booking.com']
    
    # Charger la liste des villes depuis un fichier CSV
    df = pd.read_csv("TopTown.csv")
    list_town = df['town'].head(5).tolist()  # Ne récupère que le TOP 5 des villes

    start_urls = [f"https://www.booking.com/searchresults.fr.html?ss={town}" for town in list_town]
    
    def parse(self, response):
    # Extraire les noms des hôtels et leurs liens
      hotel_names = response.xpath("//h3/a/div[1]/text()").getall()
      link_hotels = response.xpath("//h3/a/@href").getall()
    
      print(f"Found {len(hotel_names)} hotels on {response.url}")

    # Enregistrer les informations sur les hôtels
      for hotel_name, hotel_link in zip(hotel_names, link_hotels):
          # Extraire le nom de la ville à partir de la liste des villes
          town_name = response.url.split('ss=')[1].split('&')[0]  # Extraire la ville à partir de l'URL
        # Aller récupérer les informations supplémentaires pour chaque hôtel
          yield response.follow(hotel_link, callback=self.parse_hotel, meta={
                'hotel_name': hotel_name.strip(),
                'town_name': town_name  # Passer la ville dans meta
            })

  

    def parse_hotel(self, response):
      city_name = response.meta['town_name']
    # Extraire les informations spécifiques à chaque hôtel
      note_text = response.css('div.ac4a7896c7::text').get()
      description = response.css('p.a53cbfa6de.b3efd73f69::text').get()
      latlng = response.css('a[data-atlas-latlng]::attr(data-atlas-latlng)').get()

    # Si latlng est trouvé, séparer latitude et longitude
      latitude, longitude = (None, None)
      if latlng:
        latitude, longitude = latlng.split(',')

    # Retourner les informations de l'hôtel sous forme de dictionnaire
      yield {
        'town': city_name,
        'hotel_name': response.meta['hotel_name'],
        'hotel_link': response.url,
        'note': note_text.strip() if note_text else 'N/A',
        'description': description.strip() if description else 'N/A',
        'latitude': latitude,
        'longitude': longitude,
    }
# Nom du fichier où les résultats seront sauvegardés
filename = "Infos_hotel.json"

# Si le fichier existe déjà, le supprimer avant de commencer (sinon Scrapy va concaténer les résultats)
if filename in os.listdir():
    os.remove(filename)

# Déclarer un nouveau CrawlerProcess avec les paramètres
process = CrawlerProcess(settings={ 
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'LOG_LEVEL': logging.DEBUG,
    'AUTOTHROTTLE_ENABLED': True,
    'FEEDS': {filename: {"format": "json"}}  # Spécification du fichier JSON de sortie
})

# Démarrer le crawl avec le spider défini
process.crawl(KayakSpider)
process.start()

print("Crawl terminé et fichier JSON généré.")