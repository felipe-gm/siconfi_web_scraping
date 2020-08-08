import scrapy
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='last_crawl_1.log',
    filemode='w',
    level=logging.DEBUG
    )


def makeDirectory(path_to_directory):
    from pathlib import Path
    p = Path(path_to_directory)
    p.mkdir(exist_ok=True, parents=True)


class TablesSpider(scrapy.Spider):
    name = "tables_1"

    def start_requests(self):
        import pandas as pd

        url = 'http://siops.datasus.gov.br/rel_LRF.php'

        cookies = {"PHPSESSID": "9g9ilpn6jrmc9m5rt4dete0in5"}


        df = pd.read_csv('ibge.csv')
        for row in df.itertuples():
            for year in ['2019']:
                uf_cod = row._3
                mun_cod = row._1//10

                headers = {
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "http://siops.datasus.gov.br",
                    "Connection": "keep-alive",
                    "Referer": f"""http://siops.datasus.gov.br/consleirespfiscal.php?S=1&UF={
                        uf_cod 
                        };&Municipio={
                        mun_cod 
                        };&Ano={
                        year
                        }&Periodo=1""",
                    "Upgrade-Insecure-Requests": "1"
                }

                body = f'''cmbAno={
                    year
                    }&cmbUF={
                    uf_cod 
                    }&cmbPeriodo=1&cmbMunicipio%5B%5D={
                    mun_cod 
                    }&BtConsultar=Consultar'''

                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    method='POST',
                    dont_filter=True,
                    cookies=cookies,
                    headers=headers,
                    body=body,
                )

    def html_to_csv(self, table_str, path, file_name):
        from bs4 import BeautifulSoup
        import csv

        try:
            html = table_str[0].get()
            soup = BeautifulSoup(html, features="lxml")
            table = soup.find("table")

            output_rows = []
            for table_row in table.findAll('tr'):
                columns = table_row.findAll('td')
                output_row = []
                for column in columns:
                    output_row.append(column.text)
                output_rows.append(output_row)
                
            with open(path+file_name, 'w') as csvfile:
                writer = csv.writer(csvfile, delimiter = ';')
                writer.writerows(output_rows)

        except IndexError:
            self.log(
                f'''Empty response for COD UF = {
                    path[10:12]
                } and Cod Municipio = {
                    path[13:-1]
                } at year = {
                    file_name[:4]
                }''', 
                level=logging.WARNING
                )

    def parse(self, response):
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)

        request_body = response.request.body.decode()

        prefix_cod_municipio = '&cmbMunicipio%5B%5D='
        sufix_cod_municipio = '&BtConsultar='

        prefix_cod_uf = '&cmbUF='
        sufix_cod_uf = '&cmbPeriodo='

        cod_municipio = request_body[
            request_body.find(prefix_cod_municipio)+len(prefix_cod_municipio)
            :request_body.find(sufix_cod_municipio)
            ]
        cod_uf = request_body[
            request_body.find(prefix_cod_uf)+len(prefix_cod_uf)
            :request_body.find(sufix_cod_uf)
            ]

        path_raw = 'dados/raw/'
        csv_path = path_raw+f'{cod_uf}_{cod_municipio}/'

        makeDirectory(csv_path)


        prefix_year = 'cmbAno='

        year = request_body[
            request_body.find(prefix_year)+len(prefix_year)
            :request_body.find(prefix_cod_uf)
            ]

        despesas_file_name = f'{year}_despesas_sub.csv'

        despesas_xpath = '/html/body/div[2]/div[3]/div/div[1]/div/table[11]'

        despesas_table = response.xpath(despesas_xpath)

        # parsers  
        self.html_to_csv(despesas_table, csv_path, despesas_file_name)
        self.log(f'Saved file {despesas_file_name} at {csv_path}')
