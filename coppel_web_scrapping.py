# -*- coding: utf-8 -*-
"""
Código para hacer web scrapping de los productos ofertados
en la página de coppel y varias caracteristicas
https://www.coppel.com/
"""
############### web scrapping coppel ################

# cargar librerias
import pandas as pd
import numpy as np
from datetime import datetime 
from datetime import date
# Import scrapy
import scrapy 
# Import the CrawlerProcess: for running the spider
from scrapy.crawler import CrawlerProcess
import winsound
# importar sonido de alarma
duration = 1000  # milliseconds
freq = 440  # Hz


#### preparar insumos para el Spider ############################


# creamos lista con formdata para cada pagina
p_index=['0','72','144','216','288','360','432','504',
         '576']

# creamos formdata lists
form_list=[]
for i in p_index:
    params={'facet':'', 'productBeginIndex':i,
          'orderBy':'13', 'pageView':'list',
          'minPrice':'','maxPrice':'','pageSize':'72'}
    form_list.append(params)
    
# listas a llenar con datos extraidos
item_name_l=[]
p_contado_l=[]
p_credit_l=[]
quince_l=[]
ruta_l = []
category_l=[]
# del primer spider
marca_l=[]
modelo_l=[]
color_l=[]
capacidad_l=[]
tipo_refri_l=[]
tamano_l=[]
tipo_estufa_l=[]
quemadores_l=[]
medidas_l=[]
# del segundo spider
marca_l=[]
modelo_l=[]
modelo2_l=[]
operador_l=[]
sisop_l=[]
v_sisop_l=[]
camarat_l=[]
pantalla_l=[]
color_l=[]
# del tercer spider
marca_l=[]
modelo_l=[]
modelo2_l=[]
operador_l=[]
sisop_l=[]
v_sisop_l=[]
camarat_l=[]
pantalla_l=[]
color_l=[]
pulgadas_l=[]
tamanop_l=[]
procesador_l=[]
ram_l=[]
tamano_l=[]
sku_l=[]
url_item_l=[]

all_list=[]

# diccionario donde guardaremos todos los datos extraidos
df_dict=dict()

##### paginas a examinar

urls_list=[ # cocina
            'https://www.coppel.com/linea-blanca-electrodomesticos/cocina-linea-blanca/refrigeradores-y-congeladores',
            'https://www.coppel.com/linea-blanca-electrodomesticos/cocina-linea-blanca/estufas-para-cocina',
            'https://www.coppel.com/linea-blanca-electrodomesticos/cocina-linea-blanca/parrillas',
            'https://www.coppel.com/linea-blanca-electrodomesticos/cocina-linea-blanca/campanas-para-cocina',
            'https://www.coppel.com/linea-blanca-electrodomesticos/cocina-linea-blanca/microondas'
             # lavado y secado
             'https://www.coppel.com/linea-blanca-electrodomesticos/lavadoras-y-secadoras/lavadoras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/lavadoras-y-secadoras/secadoras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/lavadoras-y-secadoras/centrifugadoras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/lavadoras-y-secadoras/lavasecadoras',
             # clima y ventilacion
             'https://www.coppel.com/linea-blanca-electrodomesticos/clima-y-ventilacion',
             # limpieza
             'https://www.coppel.com/linea-blanca-electrodomesticos/limpieza/planchas',
             'https://www.coppel.com/linea-blanca-electrodomesticos/limpieza/aspiradoras',
             # electrodomesticos
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/licuadoras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/batidoras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/hornos-electricos',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/parrillas-electricas',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/sandwicheras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/tostadores',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/extractores',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/exprimidores',
             'https://www.coppel.com/linea-blanca-electrodomesticos/cafeteras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/vaporeras',
             'https://www.coppel.com/linea-blanca-electrodomesticos/electrodomesticos/maquinas-de-coser',
             # celulares
            'https://www.coppel.com/celulares/celulares-telcel',
            'https://www.coppel.com/celulares/telefonos-celulares-libres',
            'https://www.coppel.com/celulares/celulares-att',
            'https://www.coppel.com/celulares/movistar',
            # 
            'https://www.coppel.com/electronica/television-y-video/pantallas-led',
            'https://www.coppel.com/electronica/television-y-video/teatros-en-casa',
            'https://www.coppel.com/electronica/television-y-video/reproductores-blu-ray-dvd',
            # laptops
            'https://www.coppel.com/electronica/computadoras/laptops',
            'https://www.coppel.com/electronica/computadoras/computadoras-de-escritorio-computo',
            # colchones
            'https://www.coppel.com/hogar/colchones',
            # motos
            'https://www.coppel.com/automotriz/motos/motocicletas-motos'
             ]


# urls base
#urls_list = ['https://www.coppel.com/linea-blanca-electrodomesticos/cocina-linea-blanca']


############ Create the Spider class ###################
class My_Spider_1(scrapy.Spider):
  name = "my_spider_1"
  # start_requests method
  def start_requests(self):
      urls=urls_list
      for url in urls:
          for i in range(len(form_list)):
              yield scrapy.FormRequest(url = url,
                             method='GET',
                             formdata=form_list[i],
                             callback = self.parse_front)
          
  # First parsing method
  def parse_front(self, response):
      links = response.xpath( '//div[@class="image"]/a/@href').extract()
      # Follow each of the extracted links
      for link in links:
          
          yield response.follow(url=link, callback=self.parse_pages)
          
      # Second parsing method
  def parse_pages(self, response):
      # variables temporales a llenar
      item_name=''
      p_contado=''
      p_credit=''
      quince=''
      ruta=''
      sku=''
      url_item=''
      llaves=[]
      valores=[]
      carac=dict()
      
      # Extract course description
      item_name = response.css('h1.main_header::text').extract_first()
      p_contado = response.xpath('//div[@class="pcontado"]/input/@value').extract_first()
      p_credit = response.xpath('//div[@class="biweekly"]//span[contains(@id,"creditCoppel")]/text()').extract_first()
      quince = response.css('div.p_credito p::text').extract_first()
      ruta = response.xpath('//div[@id="widget_breadcrumb"]//input[@id="all_breadcrumb"]/@value').extract_first()
      llaves = response.xpath('//table[@class="table table-bordered"]//td[1]/span[1]/text()').extract()
      valores = response.xpath('//table[@class="table table-bordered"]//td[2]/span[1]/text()').extract()
      sku = response.css( 'span.sku ::text').extract_first()
      url_item=response.xpath( '//link[@rel="canonical"]/@href').extract_first()
      
      #llenamos diccionario de caracteristicas
      llaves=[t.strip() for t in llaves]
      valores=[t.strip() for t in valores]
      carac['Marca:']=['']
      carac['Modelo #:']=['']
      carac['Color:']=['']
      carac['Capacidad:']=['']
      carac['Tipo de refrigerador:']=['']
      carac['Tamaño:']=['']
      carac['Tipo de estufa:']=['']
      carac['Quemadores:']=['']
      carac['Medidas (largo x ancho x alto):']=['']
      
      carac['Marca:']=['']
      carac['Modelo:']=['']
      carac['Modelo #:']=['']
      carac['Operador:']=['']
      carac['Sistema Operativo:']=['']
      carac['Versión del sistema operativo:']=['']
      carac['Cámara trasera:']=['']
      carac['Tamaño de pantalla:']=['']
      carac['Color:']=['']
      
      carac['Marca:']=['']
      carac['Modelo:']=['']
      carac['Modelo #:']=['']
      carac['Operador:']=['']
      carac['Sistema Operativo:']=['']
      carac['Versión del sistema operativo:']=['']
      carac['Cámara trasera:']=['']
      carac['Tamaño de pantalla:']=['']
      carac['Color:']=['']
      carac['Pulgadas:']=['']
      carac['Tamaño de pantalla:']=['']
      carac['Procesador:']=['']
      carac['Memoria RAM:']=['']
      carac['Tamaño:']=['']
      
      for i in range(len(llaves)):
          carac[llaves[i]]=[valores[i]]
      
      # convertimos a lista el resto de variables
      item_name=[item_name]
      p_contado=[p_contado]
      p_credit=[p_credit]
      quince=[quince]
      ruta=[ruta]    
      sku=[sku]
      url_item=[url_item]
      
      # llenamos listas finales con los datos
      global item_name_l
      item_name_l += item_name
      global p_contado_l
      p_contado_l += p_contado
      global p_credit_l
      p_credit_l += p_credit
      global quince_l
      quince_l += quince
      global ruta_l
      ruta_l += ruta
      global sku_l
      sku_l += sku
      global url_item_l
      url_item_l += url_item
      global marca_l
      marca_l += carac['Marca:']
      global modelo_l
      modelo_l += carac['Modelo:']
      global modelo2_l
      modelo2_l += carac['Modelo #:']
      global color_l
      color_l += carac['Color:']
      global capacidad_l
      capacidad_l += carac['Capacidad:']
      global tipo_refri_l
      tipo_refri_l += carac['Tipo de refrigerador:']
      global tamano_l
      tamano_l += carac['Tamaño:']
      global tipo_estufa_l
      tipo_estufa_l += carac['Tipo de estufa:']
      global quemadores_l
      quemadores_l += carac['Quemadores:']
      global medidas_l
      medidas_l += carac['Medidas (largo x ancho x alto):']
      
      global operador_l
      operador_l += carac['Operador:']
      global sisop_l
      sisop_l += carac['Sistema Operativo:']
      global v_sisop_l
      v_sisop_l += carac['Versión del sistema operativo:']
      global camarat_l
      camarat_l += carac['Cámara trasera:']
      global pantalla_l
      pantalla_l += carac['Tamaño de pantalla:']

      global pulgadas_l
      pulgadas_l += carac['Pulgadas:']
      global procesador_l
      procesador_l += carac['Procesador:']
      global ram_l
      ram_l += carac['Memoria RAM:']


################## Run the Spider ###############
process = CrawlerProcess()
process.crawl(My_Spider_1)
process.start()

######## guardamos datos extraidos en el diccionario

df_dict['item_name']=[str(t).strip() for t in item_name_l]
df_dict['p_contado']=[str(t).strip() for t in p_contado_l]
df_dict['p_credit']=[str(t).strip() for t in p_credit_l]
df_dict['quince']=[str(t).strip() for t in quince_l]
df_dict['ruta']=[str(t).strip() for t in ruta_l]
df_dict['sku']=[str(t).strip() for t in sku_l]
df_dict['marca']=[str(t).strip() for t in marca_l]
df_dict['modelo']=[str(t).strip() for t in modelo_l]
df_dict['color']=[str(t).strip() for t in color_l]
df_dict['capacidad']=[str(t).strip() for t in capacidad_l]
df_dict['tipo_refri']=[str(t).strip() for t in tipo_refri_l]
df_dict['tamano']=[str(t).strip() for t in tamano_l]
df_dict['tipo_estufa']=[str(t).strip() for t in tipo_estufa_l]
df_dict['quemadores']=[str(t).strip() for t in quemadores_l]
df_dict['medidas']=[str(t).strip() for t in medidas_l]
df_dict['url_item']=[str(t).strip() for t in url_item_l]

df_dict['modelo2']=[str(t).strip() for t in modelo2_l]
df_dict['operador']=[str(t).strip() for t in operador_l]
df_dict['sisop']=[str(t).strip() for t in sisop_l]
df_dict['v_sisop']=[str(t).strip() for t in v_sisop_l]
df_dict['camarat']=[str(t).strip() for t in camarat_l]
df_dict['pantalla']=[str(t).strip() for t in pantalla_l]

df_dict['pulgadas']=[str(t).strip() for t in pulgadas_l]
df_dict['procesador']=[str(t).strip() for t in procesador_l]
df_dict['ram']=[str(t).strip() for t in ram_l]


############  guardamos en csv ###############################
df=pd.DataFrame(df_dict)
# agregamos fecha y hora
today_f = date.today()
today_t = datetime.today()
df['fecha']=today_f.strftime("%d/%m/%Y")
df['hora']=today_t.strftime("%H:%M:%S")

today = date.today().strftime("%Y_%m_%d")
df.to_csv('F:/practicas_python/web_scapping/buen_fin/coppel_{0}_{1}.csv'.format('all_categories',today))
winsound.Beep(freq, duration)

