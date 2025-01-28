import scrapy
from scrapy.crawler import CrawlerProcess
import geopandas as gpd
import pandas as pd
from scraper_tools import busca_geometria, busca_cep


# Criar Spider
class DfimoveisHousesSpider(scrapy.Spider):
    name = "dfimoveis"

    # Configurações
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 10
    }

    # Dict para receber os dados
    data_dict = {"preco":[], "metragem":[], "total_quartos":[], "suites":[], "vagas":[], "endereco":[], "bairro":[], "cep":[] ,"url":[]}

    # Número da página
    pagina = 1

    # Scraper
    def start_requests(self):
        yield scrapy.Request(f"https://www.dfimoveis.com.br/aluguel/df/brasilia/apartamento?pagina={self.pagina}")

    def parse(self, response):
        ads = response.xpath('//*[@id="resultadoDaBuscaDeImoveis"]/a')

        if len(ads) > 0:
            # Iterar sobre os anúncios para obter as informações
            for ad in ads:
                url = response.urljoin(response.xpath('//a[@class="new-card"]/@href').get())    # Está com erro
                preco = int(ad.xpath('.//div[@class="new-price"]/h4/span/text()').get().replace(".", ""))
                endereco = ad.xpath('.//h2/text()').get().strip()
                bairro = endereco.split(", ")[1]
                cep = None     # cep = busca_cep(endereco.split(", ")[0]) Não está funcionando
                metragem = int(ad.xpath('.//div[@class="new-price-detail"]/ul/li[@class="m-area"]/span/text()').get().split(" ")[0])   
                
                # Quartos
                quartos = ad.xpath('.//div[@class="new-price-detail"]/ul/li[contains(., "Quarto")]/span/text()').get()
                if quartos is None:
                    quartos = 0
                else:
                    quartos = int(quartos.split(" ")[0])

                # Suítes
                suites = ad.xpath('.//div[@class="new-price-detail"]/ul/li[contains(., "Suíte")]/span/text()').get()
                if suites is None:
                    suites = 0
                else:
                    suites = int(suites.split(" ")[0])

                # Vagas
                vagas = ad.xpath('.//div[@class="new-price-detail"]/ul/li[contains(., "Vaga")]/span/text()').get()
                if vagas is None:
                    vagas = 0
                else:
                    vagas = int(vagas.split(" ")[0])

                # Alimentar o dict
                self.data_dict["preco"] += [preco]
                self.data_dict["metragem"] += [metragem]
                self.data_dict["endereco"] += [endereco]
                self.data_dict["total_quartos"] += [quartos]
                self.data_dict["suites"] += [suites]
                self.data_dict["vagas"] += [vagas]
                self.data_dict["cep"] += [cep]
                self.data_dict["url"] += [url]
                self.data_dict["bairro"] += [bairro]

        # Próxima página
        self.pagina += 1

        # Ao chegar numa página sem anúncios, cria um geodataframe e executa a geocodificação
        if len(ads) > 0:
            yield scrapy.Request(f"https://www.dfimoveis.com.br/aluguel/df/brasilia/apartamento?pagina={self.pagina}", callback=self.parse)
        else:
            # Encerrar processo de scraping e iniciar geocodificação
            df = pd.DataFrame(self.data_dict)
            df['geometry'] = df.apply(busca_geometria, axis=1)
            gdf_saida = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

            # Exportar arquivo geojson
            gdf_saida.to_file("alugueis_dfimoveis.geojson")

            # Encerrar processo
            return

# Configurando o processo de execução do Scrapy
process = CrawlerProcess()
process.crawl(DfimoveisHousesSpider)
process.start()
