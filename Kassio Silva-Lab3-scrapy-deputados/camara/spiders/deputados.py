import codecs
import json
from datetime import datetime
from functools import reduce

import scrapy
import bs4

from camara.utils.convert import parse_brl_num_to_float


class DeputiesSpider(scrapy.Spider):
    name = "camara"

    def start_requests(self):
        lista_deputadas = 'deputadas.txt'
        lista_deputados = 'deputados.txt'
        base = 'camara/assets'
        
        with open(f'{base}/{lista_deputadas}', 'r') as female:
            lines = female.readlines()

            for line in lines:
                url = line.split('"')[1]
                yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(gender='feminino'))

        with open(f'{base}/{lista_deputados}', 'r') as male:
            lines = male.readlines()

            for line in lines:
                url = line.split('"')[1]
                yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(gender='masculino'))

    def parse(self, response, gender):
        informacoes_pessoais_deputado = self.informacoes_pessoais_deputado(response)
        gastos_parlamentar = self.gastos_parlamentar(response)
        gastos_gabinete = self.gastos_gabinete(response)
        salario_bruto = self.salario_bruto(response)
        presenca = self.presenca(response)

        yield {
            "genero": gender,
            **informacoes_pessoais_deputado,
            **gastos_parlamentar,
            **gastos_gabinete,
            **presenca,
            "salario_bruto": salario_bruto
        }

    def presenca(self, response):
        def parse_presenca_to_int(presenca):
            return int(presenca.strip().split(" ")[0].strip())
        soup = bs4.BeautifulSoup(response.body, "html")
        presencas = soup.find_all("dd", {"class": "list-table__definition-description"})


        if (presencas):
            return {
                "presenca_plenario": parse_presenca_to_int(presencas[0].get_text()),
                "ausencia_plenario": parse_presenca_to_int(presencas[1].get_text()) + parse_presenca_to_int(presencas[2].get_text()),
                "ausencia_justificada_plenario":  parse_presenca_to_int(presencas[2].get_text()),
                "presenca_comissao": parse_presenca_to_int(presencas[3].get_text()),
                "ausencia_comissao": parse_presenca_to_int(presencas[4].get_text()) + parse_presenca_to_int(presencas[5].get_text()),
                "ausencia_justificada_comissao": parse_presenca_to_int(presencas[5].get_text())
            }

        return {
            "presenca_plenario": 0,
            "ausencia_plenario": 0,
            "ausencia_justificada_plenario":  0,
            "presenca_comissao": 0,
            "ausencia_comissao": 0,
            "ausencia_justificada_comissao": 0
        }
        
    def salario_bruto(self, response):
        soup = bs4.BeautifulSoup(response.body, "html")
        salario_bruto = soup.find_all("section", {"id": "recursos-section"})[0].find_all("a", {"class": "beneficio__info"})[1]
        return parse_brl_num_to_float(salario_bruto.get_text().split("R$")[1].strip())
    
    def informacoes_pessoais_deputado(self, response):
        soup = bs4.BeautifulSoup(response.body, "html")
        informacoes_deputado = soup.find_all("ul", {"class": "informacoes-deputado"})[0].find_all("li")
        data_nascimento = ''
        nome = ''

        for info in informacoes_deputado:
            if ("Data de Nascimento:" in info.get_text()):
                data_nascimento = datetime.strptime(info.get_text()[20:], '%d/%m/%Y').date().isoformat()
            
            if ("Nome Civil:" in info.get_text()):
                nome = info.get_text()[12:]
        
        return {
            "nome": nome,
            "data_nascimento": data_nascimento
        }

    def gastos_parlamentar(self, response):
        soup = bs4.BeautifulSoup(response.body, "html")
        gastos_parlamentar = soup.find_all("table", {"id": "gastomensalcotaparlamentar"})[0].find_all("td")
  
        gastos_serializados = self.parse_gastos(gastos_parlamentar)
        soma_gastos = self.soma_gastos(gastos_serializados)
        
        return {
            "gasto_total_gab": soma_gastos,
            "gasto_jan_gab": gastos_serializados[0]["gasto"] if gastos_serializados[0:] else 0.0,
            "gasto_fev_gab": gastos_serializados[1]["gasto"] if gastos_serializados[1:] else 0.0,
            "gasto_mar_gab": gastos_serializados[2]["gasto"] if gastos_serializados[2:] else 0.0,
            "gasto_abr_gab": gastos_serializados[3]["gasto"] if gastos_serializados[3:] else 0.0,
            "gasto_maio_gab": gastos_serializados[4]["gasto"] if gastos_serializados[4:] else 0.0,
            "gasto_junho_gab": gastos_serializados[5]["gasto"] if gastos_serializados[5:] else 0.0,
            "gasto_jul_gab": gastos_serializados[6]["gasto"] if gastos_serializados[6:] else 0.0,
            "gasto_agosto_gab": gastos_serializados[7]["gasto"] if gastos_serializados[7:] else 0.0,
            "gasto_set_gab": gastos_serializados[8]["gasto"] if gastos_serializados[8:] else 0.0,
            "gasto_out_gab": gastos_serializados[9]["gasto"] if gastos_serializados[9:] else 0.0,
            "gasto_nov_gab": gastos_serializados[10]["gasto"] if gastos_serializados[10:] else 0.0,
            "gasto_dez_gab": gastos_serializados[11]["gasto"] if gastos_serializados[11:] else 0.0,
        }

    def gastos_gabinete(self, response):
        soup = bs4.BeautifulSoup(response.body, "html")
        gastos_gabinete = soup.find_all("table", {"id": "gastomensalverbagabinete"})[0].find_all("td")
  
        gastos_serializados = self.parse_gastos(gastos_gabinete)
        soma_gastos = self.soma_gastos(gastos_serializados)

        return {
            "gasto_total_par": soma_gastos,
            "gasto_jan_par": gastos_serializados[0]["gasto"] if gastos_serializados[0:] else 0.0,
            "gasto_fev_par": gastos_serializados[1]["gasto"] if gastos_serializados[1:] else 0.0,
            "gasto_mar_par": gastos_serializados[2]["gasto"] if gastos_serializados[2:] else 0.0,
            "gasto_abr_par": gastos_serializados[3]["gasto"] if gastos_serializados[3:] else 0.0,
            "gasto_maio_par": gastos_serializados[4]["gasto"] if gastos_serializados[4:] else 0.0,
            "gasto_junho_par": gastos_serializados[5]["gasto"] if gastos_serializados[5:] else 0.0,
            "gasto_jul_par": gastos_serializados[6]["gasto"] if gastos_serializados[6:] else 0.0,
            "gasto_agosto_par": gastos_serializados[7]["gasto"] if gastos_serializados[7:] else 0.0,
            "gasto_set_par": gastos_serializados[8]["gasto"] if gastos_serializados[8:] else 0.0,
            "gasto_out_par": gastos_serializados[9]["gasto"] if gastos_serializados[9:] else 0.0,
            "gasto_nov_par": gastos_serializados[10]["gasto"] if gastos_serializados[10:] else 0.0,
            "gasto_dez_par": gastos_serializados[11]["gasto"] if gastos_serializados[11:] else 0.0,
        }

    def parse_gastos(self, tabela):
        gastos = []

        for td in range(0, len(tabela), 3):
            gastos.append({
                "mes": tabela[td].get_text(),
                "gasto": parse_brl_num_to_float(tabela[td + 1].get_text())
            })
        
        return gastos

    def soma_gastos(self, gastos):
        gastos_valor = [gasto["gasto"] for gasto in gastos]
        return reduce(lambda a, b: a+b, gastos_valor)


    def save_csv(self, objeto):
        write_object = json.dumps(objeto, indent=4, ensure_ascii=False)