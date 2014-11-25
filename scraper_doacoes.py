from pandas import *
from pymongo import MongoClient
import urllib.parse, urllib.request
import math
from urllib.request import urlopen
from bs4 import BeautifulSoup
from functools import wraps
import errno
import os
import signal
import re
import hashlib
import json

#classe para definir um decorator timeout para uma funçao (se ela não terminar em X segundos, chama uma exceção)
class TimeoutError(Exception):
    pass

def timeout(seconds=120, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator

def acha_comites():
    estados = {
        "AL":"AL",
        "AM":"AM",
        "AP":"AP",
        "BA":"BA",
        "BR":"BR",
        "CE":"CE",
        "DF":"DF",
        "ES":"ES",
        "GO":"GO",
        "MA":"MA",
        "MG":"MG",
        "MS":"MS",
        "MT":"MT",
        "PA":"PA",
        "PB":"PB",
        "PE":"PE",
        "PI":"PI",
        "PR":"PR",
        "RJ":"RJ",
        "RN":"RN",
        "RO":"RO",
        "RR":"RR",
        "RS":"RS",
        "SC":"SC",
        "SE":"SE",
        "SP":"SP",
        "TO":"TO"        
    }
    
    partidos = {
        "10":"PRB",
        "11":"PP",
        "12":"PDT",
        "13":"PT",
        "14":"PTB",
        "15":"PMDB",
        "16":"PSTU",
        "17":"PSL",
        "19":"PTN",
        "20":"PSC",
        "21":"PCB",
        "22":"PR",
        "23":"PPS",
        "25":"DEM",
        "27":"PSDC",
        "28":"PRTB",
        "29":"PCO",
        "31":"PHS",
        "33":"PMN",
        "36":"PTC",
        "40":"PSB",
        "43":"PV",
        "44":"PRP",
        "45":"PSDB",
        "50":"PSOL",
        "51":"PEN",
        "54":"PPL",
        "55":"PSD",
        "65":"PC do B",
        "70":"PT do B",
        "77":"SD",
        "90":"PROS"     
    }
    
    resultado = {}
    i = 0
    for u in estados:
        for p in partidos:   
            url = "http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/listaComiteDirecaoPartidaria.action?nrPartido="+p+"&siglaUf="+u+"&municipio="
            page = le_pagina(url)
            links = page.findAll("a")
            codigos = [p[0].replace("'","") for p in [palavra[1].split(",") for palavra in [str(l).split("javascript:passaValor(") for l in links]]]
            for c in codigos:
                print(partidos[p] + " - "+u+" - "+c)


def roda_candidatos():
    colecao = conecta("candidatos3")
    antigas = doacoes_antigas("candidatos3")
    colecao.remove()
    candidatos = le_candidatos()
    codigos = [str(c) for c in list(candidatos["sequencial"])]

    for c in codigos:
        c = str(c)
        url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoReceitasByCandidato.action?sqCandidato='+c+'&tipoEntrega=0'
        page = faz_post(url)
        linhas = page.findAll("tr", {"class":re.compile("linha.*")})
        for l in linhas:
            campos = l.findAll("td")
            i = [c.string.strip() for c in campos]
            codigo = acha_codigo(i[0],i[4],i[5],i[6],i[7],i[9])
            if codigo not in antigas:        
                doacao = {}
                doacao["doador"] = i[0]
                doacao["cpf_cnpj"] = i[1]
                doacao["doador_orig"] = i[2]
                doacao["cpf_cnpj_orig"] = i[3]
                doacao["data"] = i[4]
                doacao["recibo"] = i[5]
                doacao["valor"] = i[6]
                doacao["especie"] = i[7]
                doacao["documento"] = i[8]
                doacao["candidato"] = i[9]
                doacao["numero"] = i[10]
                doacao["partido"] = i[11]
                doacao["cargo"] = i[12]
                doacao["uf"] = i[13]
                doacao["fonte"] = i[14]    
                doacao["codigo"] = codigo
                doacao["sq"] = c
                colecao.insert(doacao)
                print(doacao)
            else:
                print("Doação repetida - "+str(i[5]))

def despesa_candidatos():
   colecao = conecta("desp_candidatos3")
   colecao.remove()
   antigas = doacoes_antigas("desp_candidatos3")
   candidatos = le_candidatos()
   codigos = [str(c) for c in list(candidatos["sequencial"])]
   dados_dict = candidatos.set_index('sequencial').to_dict()
   for c in codigos:
        c = str(c)
        url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoDespesasByCandidato.action?sqCandidato='+c+'&tipoEntrega=0'
        page = faz_post(url)
        linhas = page.findAll("tr", {"class":re.compile("linha.*")})
        for l in linhas:
            campos = l.findAll("td")
            i = [c.string.strip() for c in campos]
            codigo = acha_codigo(i[0],i[4],i[5],i[6],i[7],i[9])
            if codigo not in antigas:        
                doacao = {}
                doacao["doador"] = i[0]
                doacao["cpf_cnpj"] = i[1]
                doacao["doador_orig"] = i[2]
                doacao["cpf_cnpj_orig"] = i[3]
                doacao["data"] = i[4]
                doacao["despesa"] = i[5]
                doacao["valor"] = i[6]
                doacao["beneficiario"] = i[7]
                doacao["especie_documento"] = i[8]
                doacao["num_documento"] = i[9]
                doacao["partido"] = dados_dict["partido"][int(c)]
                doacao["uf"] = dados_dict["uf"][int(c)]
                doacao["cargo"] = dados_dict["cargo"][int(c)]
                doacao["codigo"] = codigo
                doacao["sq"] = c
                colecao.insert(doacao)
                print(doacao)
            else:
                print("Gasto repetido - "+str(i[5]))
                
def despesa_comites():
   colecao = conecta("desp_comites3")
   colecao.remove()
   antigas = doacoes_antigas("desp_comites3")
   comites = le_comites()
   codigos = [str(c) for c in list(comites["codigo"])]
   dados_dict = comites.set_index('codigo').to_dict()
   for c in codigos:
       c = str(c)
       url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoDespesasByComite.action?sqComiteFinanceiro='+c+'&tipoEntrega=0'
       page = faz_post(url)
       linhas = page.findAll("tr", {"class":re.compile("linha.*")})
       for l in linhas:
           campos = l.findAll("td")
           i = [c.string.strip() for c in campos]
           codigo = acha_codigo(i[0],i[4],i[5],i[6],i[7],i[9])
           if codigo not in antigas:        
               doacao = {}
               doacao["doador"] = i[0]
               doacao["cpf_cnpj"] = i[1]
               doacao["doador_orig"] = i[2]
               doacao["cpf_cnpj_orig"] = i[3]
               doacao["data"] = i[4]
               doacao["despesa"] = i[5]
               doacao["valor"] = i[6]
               doacao["beneficiario"] = i[7]
               doacao["especie_documento"] = i[8]
               doacao["num_documento"] = i[9]
               doacao["partido"] = dados_dict["partido"][int(c)]
               doacao["uf"] = dados_dict["estado"][int(c)]
               doacao["codigo"] = codigo
               doacao["sq"] = c
               colecao.insert(doacao)
               print(doacao)
           else:
               print("Gasto repetido - "+str(i[5]))    

def roda_comites():
    colecao = conecta("comites3")
    antigas = doacoes_antigas("comites3")
    colecao.remove()
    comites = le_comites()
    codigos = [str(c) for c in list(comites["codigo"])]
    for c in codigos:
        c = str(c)
        url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoReceitasByComite.action?sqComiteFinanceiro='+c+'&tipoEntrega=0'
        print(url)
        page = faz_post(url)
        linhas = page.findAll("tr", {"class":re.compile("linha.*")})
        for l in linhas:
            campos = l.findAll("td")
            i = [c.string.strip() for c in campos]
            codigo = acha_codigo(i[0],i[4],i[5],i[6],i[7],i[9])
            if codigo not in antigas:                    
                doacao = {}
                doacao["doador"] = i[0]
                doacao["cpf_cnpj"] = i[1]
                doacao["doador_orig"] = i[2]
                doacao["cpf_cnpj_orig"] = i[3]
                doacao["data"] = i[4]
                doacao["recibo"] = i[5]
                doacao["valor"] = i[6]
                doacao["especie"] = i[7]
                doacao["documento"] = i[8]
                doacao["comite"] = i[9]
                doacao["partido"] = i[10]
                doacao["uf"] = i[11]
                doacao["fonte"] = i[12]
                doacao["codigo"] = codigo
                doacao["sq"] = c
                colecao.insert(doacao)
                print(doacao)
            else:
                print("Doação repetida - "+str(i[5]))    
                

def faz_post(url):
    while True:
        try:
            print(url)
            page = abre_pagina(url)
            break
        except:
            print("Erro no código: "+url)
            continue
    return page       

@timeout()
def faz_req(req):
    response = urllib.request.urlopen(req)
    page = BeautifulSoup(response.read())
    return page

def le_pagina(url):
    #loop para continuar tentando
    while True:
        try:
            print(url)
            page = abre_pagina(url)
            break
        except TimeoutError:
            print("Erro aqui: "+url)
            continue
    return page

#se não abrir em 3 segundos, ele levanta uma exceção
@timeout()
def abre_pagina(url):
    return BeautifulSoup(urlopen(url).read())

def conecta(db):
    client = MongoClient()
    my_db = client["doacoes2014"]
    my_collection = my_db[db]
    return my_collection

def doacoes_antigas(db):
    my_collection = conecta(db)
    resultado = my_collection.find()
    return [r["codigo"] for r in resultado]

#retorna apenas os que faltam ser pesquisados
def faz_consulta(db):
    colecao = conecta(db)
    resultado = colecao.find()
    resultado = [r for r in resultado]
    path = os.path.dirname(os.path.abspath(__file__))
    dados = DataFrame(resultado)
    if not dados.empty:
        try:
            del dados["codigo"]
        except:
            pass
        del dados["_id"]
        dados.to_csv(path+"/"+db+".csv",index=False)
    return dados
    
def acha_codigo(a,b,c,d,e,f):
    return hashlib.md5((a+b+c+d+e+f).encode()).hexdigest()

def le_candidatos():
    path = os.path.dirname(os.path.abspath(__file__))
#    dados = read_csv(path+"/deps_sens.csv")
    dados = read_csv(path+"/numeros_candidatos.csv")
    sqs = read_csv("sqs.csv",header=None)
    sqs.columns = ["sqs"]
    sqs = list(sqs["sqs"])
    sqs = [int(s) for s in sqs]
    dados["sequencial"] = dados["sequencial"].apply(int)
    dados = dados[~dados.sequencial.isin(sqs)]
    print(dados)

    return dados

def le_comites():
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/numeros_comites.csv")
    return dados

def compara_arquivos():
    path = os.path.dirname(os.path.abspath(__file__))
    comites = read_csv(path+"/comites.csv")
    candidatos = read_csv(path+"/candidatos.csv")
    comites = comites.fillna("")
    comites["doador"] = comites.apply(lambda t:t["doador_orig"] if (t["doador_orig"]) else t["doador"],axis=1)
    comites.to_csv(path+"/comites2.csv")
    candidatos = candidatos.fillna("")
    candidatos["doador"] = candidatos.apply(lambda t:t["doador_orig"] if (t["doador_orig"]) else t["doador"],axis=1)
    candidatos.to_csv(path+"/candidatos2.csv")

def uniformiza_cnpj():
    path = os.path.dirname(os.path.abspath(__file__))
    comites = read_csv(path+"/comites.csv")
    comites = comites.fillna("")
    comites["doador"] = comites.apply(lambda t:t["doador_orig"] if (t["doador_orig"]) else t["doador"],axis=1)
    comites["cpf_cnpj"] = comites.apply(lambda t:t["cpf_cnpj_orig"] if (t["doador_orig"]) else t["cpf_cnpj"],axis=1)
    comites["cpf_cnpj_real"] = comites.apply(lambda t:t["cpf_cnpj"].split("/")[0] if t["cpf_cnpj"].split("/")[0] else t["cpf_cnpj"],axis=1)
    comites["cpf_cnpj"] = comites["cpf_cnpj_real"]
    del comites["cpf_cnpj_real"]
    #para cada cnpj, coloca apenas o que mais aparece
    cnpjs = {}
    for c in set(comites["cpf_cnpj"]):
        if c:
            temp = comites[comites.cpf_cnpj == c]
            lista = list(temp.groupby("doador").agg({"recibo":Series.nunique}).sort("recibo",ascending=0).index)
            comites["doador"][comites.cpf_cnpj == c] = comites[comites.cpf_cnpj == c]["doador"].apply(lambda t:lista[0])
    
    comites.to_csv(path+"/comites3.csv")

    candidatos = read_csv(path+"/candidatos4.csv")
    candidatos = candidatos.fillna("")
    candidatos["doador"] = candidatos.apply(lambda t:t["doador_orig"] if (t["doador_orig"]) else t["doador"],axis=1)
    candidatos["cpf_cnpj"] = candidatos.apply(lambda t:t["cpf_cnpj_orig"] if (t["doador_orig"]) else t["cpf_cnpj"],axis=1)
    candidatos["cpf_cnpj_real"] = candidatos.apply(lambda t:t["cpf_cnpj"].split("/")[0] if t["cpf_cnpj"].split("/")[0] else t["cpf_cnpj"],axis=1)
    candidatos["cpf_cnpj"] = candidatos["cpf_cnpj_real"]
    del candidatos["cpf_cnpj_real"]
    #para cada cnpj, coloca apenas o que mais aparece
    cnpjs = {}
    for c in set(candidatos["cpf_cnpj"]):
        if c:
            temp = candidatos[candidatos.cpf_cnpj == c]
            lista = list(temp.groupby("doador").agg({"recibo":Series.nunique}).sort("recibo",ascending=0).index)
            candidatos["doador"][candidatos.cpf_cnpj == c] = candidatos[candidatos.cpf_cnpj == c]["doador"].apply(lambda t:lista[0])
    
    candidatos.to_csv(path+"/candidatos4.csv")
    
    
def doadores_diferentes():
    path = os.path.dirname(os.path.abspath(__file__))
    comites = read_csv(path+"/comites.csv")
    candidatos = read_csv(path+"/candidatos.csv")
    sem_partido = candidatos[candidatos.doador.str.startswith(("Direção","Comitê")) == False]
    eleicoes = candidatos[candidatos.doador.str.lower().str.startswith(("eleição","eleicao","eleiçao","eleicão","eleições","eleicoes","eleicões","eleiçoes","elecao","eleção","eleções","eleiçã0"))]
    eleicoes["doador_real"] = eleicoes.apply(checa_doador,axis=1)
    eleicoes["igual"] = eleicoes.apply(lambda t:str(t["candidato"]).strip() == str(t["doador_real"]).strip(), axis=1)
    diferentes = eleicoes[eleicoes.igual == False]
    diferentes["valor"] = diferentes["valor"].apply(float)
    print(sum(diferentes["valor"]))
    
def checa_doador(t):
    campos = str(t["doador"]).split(" ")
    doador = ""
    for c in campos:
        if c.lower() not in ["eleição","eleicao","eleiçao","eleicão","eleições","eleicoes","eleicões","eleiçoes","elecao","eleção","eleções","eleiçã0","2014","-","eleição2014","eleições2014","eleição2014-"]:
            if c.lower() in ["senador","deputado","governador","presidente","dep."]:
                break
            doador = doador + c + " "
    return doador

def checa_despesas():
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/despesa_candidatos1.csv")
    dados2 = read_csv(path+"/despesa_candidatos2.csv")
    ufs = list((set(dados2["uf"])))
    dados = dados[dados.uf.isin(ufs)]
    print(len(dados))
    print(len(dados2))
    print("Diferença no num de doações: "+str(len(dados2)-len(dados)))
    print("*********")
    seqs = list(set(dados["sq"]))
    seqs2 = list(set(dados2["sq"]))
    print(len(seqs))
    print(len(seqs2))
    print("Diferença no num de sequenciais: "+str(len(seqs2)-len(seqs)))
    print("*********")
    dif_seqs = [s for s in seqs2 if str(s) not in seqs]
    dados_faltam = dados2[dados2["sq"].isin(dif_seqs)]
    dados_faltam.to_csv(path+"/despesas_faltam.csv")
    

    

def lista_cnpjs():
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/cnpjs.csv")    
    #tira as doações de comitês, direçÕes estaduais ou nacionais de partidos, etc
    #    comites = ["COMITE FINANCEIRO","COMITÊ FINANCEIRO","DIRECAO NACIONAL","DIREÇÃO NACIONAL","DIREÇÃO ESTADUAL","DIRECAO ESTADUAL","ELEIÇÕES 2014","ELEICOES 2014","ELEIÇÃO 2014","ELEICAO 2014"]
    #    padrao = '|'.join(comites)
    #    saem = list(dados.doador.str.upper().str.contains(padrao))
    #    ficam = [not i for i in saem]
    #    dados = dados[ficam]    
    #agora tira a coluna de partidos (não será necessária por agora)
    #del dados["partido"]
    
    #tira as linhas duplicadas
    dados.drop_duplicates(cols = 'cnpj', inplace = True)
    
    #transforma em dicionário
    dados.index = dados["cnpj"]
    del dados["cnpj"]
    dados = dados.to_dict()
    
    return dados["doador"]
    
def comite_cnpj():
    #pega a lista de cnpjs
    dados = lista_cnpjs()
    antigas = doacoes_antigas("comite_cnpj")
    
    #conecta na base certa
    colecao = conecta("comite_cnpj")
    
    url_base = "http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoReceitasByComite.action?cpfCnpjDoador="
    
    #faz consulta para cada cnpj
    for c in dados:
        url = url_base + str(c)
        page = faz_post(url)
        linhas = page.findAll("tr", {"class":re.compile("linha.*")})
        for l in linhas:
            campos = l.findAll("td")
            i = [c.string.strip() for c in campos]
            codigo = acha_codigo(dados[c],i[4],i[5],i[6],i[7],i[9])
            if codigo not in antigas:
                doacao = {}
                doacao["doador"] = dados[c]
                doacao["cpf_cnpj"] = i[1]
                doacao["doador_orig"] = i[2]
                doacao["cpf_cnpj_orig"] = i[3]
                doacao["data"] = i[4]
                doacao["recibo"] = i[5]
                doacao["valor"] = i[6]
                doacao["especie"] = i[7]
                doacao["documento"] = i[8]
                doacao["receptor"] = i[9]
                doacao["partido"] = i[10]
                doacao["uf"] = i[11]
                doacao["fonte"] = i[12]    
                doacao["codigo"] = codigo
                colecao.insert(doacao)
                print(doacao)
            else:
                print("Doação repetida - "+str(i[5]))

def candidato_cnpj():
    #pega a lista de cnpjs
    dados = lista_cnpjs()
    antigas = doacoes_antigas("candidato_cnpj")
    
    #conecta na base certa
    colecao = conecta("candidato_cnpj")
    
    url_base = "http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoReceitasByCandidato.action?sqCandidato=&sgUe=&rb1=on&rbTipo=on&tipoEntrega=2&nrCandidato=&nmCandidato=&sgUfMunicipio=&sgPartido=&action%3AresumoReceitasByCandidato=Resumo%20Response%20Headersview%20source&cpfCnpjDoador="
    
    #faz consulta para cada cnpj
    for c in dados:
        url = url_base + str(c)
        page = faz_post(url)
        linhas = page.findAll("tr", {"class":re.compile("linha.*")})
        for l in linhas:
            campos = l.findAll("td")
            i = [c.string.strip() for c in campos]
            codigo = acha_codigo(dados[c],i[4],i[5],i[6],i[7],i[9])
            if codigo not in antigas:
                doacao = {}
                doacao["doador"] = dados[c]
                doacao["cpf_cnpj"] = i[1]
                doacao["doador_orig"] = i[2]
                doacao["cpf_cnpj_orig"] = i[3]
                doacao["data"] = i[4]
                doacao["recibo"] = i[5]
                doacao["valor"] = i[6]
                doacao["especie"] = i[7]
                doacao["documento"] = i[8]
                doacao["receptor"] = i[9]
                doacao["numero"] = i[10]
                doacao["partido"] = i[11]        
                doacao["cargo"] = i[12]
                doacao["uf"] = i[13]
                doacao["fonte"] = i[14]    
                doacao["codigo"] = codigo
                colecao.insert(doacao)
                print(doacao)
            else:
                print("Doação repetida - "+str(i[5]))

def acha_cnpjs():
    #pega lista de cnpjs no banco de dados
    cand = faz_consulta("candidatos2")
    cand.index = cand["cpf_cnpj"]
    cand = cand["doador"]
    cand = cand.to_dict()

    com = faz_consulta("comites2")
    com.index = com["cpf_cnpj"]
    com = com["doador"]
    com = com.to_dict()

    cnpjs = list(cand.keys()) + list(com.keys())
    cnpjs = set(cnpjs)
    
    #faz um dicionário de tradução entre cnpjs e nomes
    traducao = dict(list(cand.items()) + list(com.items()))

    #agora compara com os que temos no arquivo armazenado
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/doacoes_burga_limas.csv")
    cnpjs2 = set(list(dados["cnpj"]))
    
    #cnpjs que faltam
    faltam = [c for c in cnpjs if c not in cnpjs2]
    
    #manda para o aruqivo
    saida = DataFrame(faltam)
    saida.columns = ["cnpj"]
    saida["doador"] = saida["cnpj"].apply(lambda t:traducao[t])
    
    #escreve
    saida.to_csv(path+"/cnpjs_faltam.csv",index=False)

def totaliza_doacoes():
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/dep_fed_eleitos.csv")
    dados["pf_pj"] = dados["cpf_cnpj"].apply(lambda t:"pj" if "/" in str(t) else "pf")
    dados["valor"] = dados["valor"].apply(lambda t:float(t.replace(",",".")))
    tabela = pivot_table(dados, values="valor",index=["doador","partido"], aggfunc=np.sum)
    tabela.to_csv(path+"/tabela.csv")
    print(tabela)
    print(sum(tabela))

def codigo_abramo(linha):
    return acha_codigo2(str(linha[1]).strip(),linha[2].strip(),linha[3].strip(),linha[4].strip(),str(linha[7]).strip())

def codigo_burga(linha):
    return acha_codigo2(str(linha[1]).strip(),linha[5].strip(),linha[6].strip(),linha[7].strip(),str(linha[12]).strip())

def acha_codigo2(a,b,c,d,e):
    return hashlib.md5((a+b+c+d+e).encode()).hexdigest()
    
def traduz_estado(uf):
    traducao = {
        "BRASIL":"BR",
        "ACRE":'AC',
        "ALAGOAS":'AL',
        "AMAPÁ":'AP',
        "AMAZONAS":'AM',
        "BAHIA":'BA',
        "CEARÁ":'CE',
        "DISTRITO FEDERAL":'DF',
        "ESPÍRITO SANTO":'ES',
        "GOIÁS":'GO',
        "MARANHÃO":'MA',
        "MATO GROSSO":'MT',
        "MATO GROSSO DO SUL":'MS',
        "MINAS GERAIS":'MG',
        "PARÁ":'PA',
        "PARAÍBA":'PB',
        "PARANÁ":'PR',
        "PERNAMBUCO":'PE',
        "PIAUÍ":'PI',
        "RIO DE JANEIRO":'RJ',
        "RIO GRANDE DO NORTE":'RN',
        "RIO GRANDE DO SUL":'RS',
        "RONDÔNIA":'RO',
        "RORAIMA":'RR',
        "SANTA CATARINA":'SC',
        "SÃO PAULO":'SP',
        "SERGIPE":'SE',
        "TOCANTINS":'TO'        
    }
    return traducao[uf] if uf in traducao else uf

def my_round(x):
    return int(x + math.copysign(0.5, x))

def compara2():
    path = os.path.dirname(os.path.abspath(__file__))
    abramo = read_csv(path+"/abramo_teste.csv")
    burga = read_csv(path+"/burga_teste.csv")
    codigos_abramo = set(abramo["codigo"])
    print(len(codigos_abramo))
    codigos_burga = set(burga["codigo"])
    print(len(codigos_burga))
    
    codigos = [c for c in codigos_burga if c not in codigos_abramo]
    codigos = list(codigos)
    
    print(len(codigos))
    

def compara_doacoes():    
    path = os.path.dirname(os.path.abspath(__file__))
    abramo = read_csv(path+"/doacoes_abramo_limas.csv")
    abramo["cnpj"] = abramo["cnpj"].apply(lambda t:(t.replace("'","")))
    abramo["codigo"] = abramo.apply(codigo_abramo,axis=1)
    print(abramo[abramo.cnpj == "30877881200"])
    
    print(abramo.columns)
    
    burga = read_csv(path+"/doacoes_burga_limas.csv")
    burga["valor"] = burga["valor"].apply(lambda t:my_round(float(t.replace(",","."))))
    burga["cnpj"] = burga["cnpj"].apply(lambda t:(t.replace(".","").replace("/","").replace("-","")))
    burga = burga[burga.uf != "CANOAS"]
    burga["uf"] = burga["uf"].apply(traduz_estado)
    burga["codigo"] = burga.apply(codigo_burga,axis=1)
    print(burga[burga.cnpj == "30877881200"])

    #    print(burga)
    abramo.to_csv("abramo_teste.csv")
    burga.to_csv("burga_teste.csv")
    
    #    recept = read_csv(path+"/burga_total.csv")
    #    recept["cnpj"] = recept["cpf_cnpj"].apply(lambda t:str(t).replace(".","").replace("/","").replace("-",""))
    
    codigos_abramo = set(abramo["codigo"])
    print(len(codigos_abramo))
    codigos_burga = set(burga["codigo"])
    print(len(codigos_burga))
    
    codigos = []
    for c in codigos_burga:
        if c == "ba6d717e7c07fba97267bc3958a521da":
            print(c)
            print(c in codigos_abramo)
        if c not in codigos_abramo:
            codigos.append(c)

    if "ba6d717e7c07fba97267bc3958a521da" in codigos:
        print("rola")
        
    print(len(codigos))
    
    saida = burga[burga.codigo.isin(codigos)]
    saida.to_csv(path+"/saida.csv")


def arrumaTitulo(t):
    return str(t).lower().title()

def traduz_doador(t):
    traducao = {   
        'Rodrigo Otavio Soares Pacheco Deputado Federal':'Rodrigo Otavio Soares Pacheco',
        'Votorantim Cimentos S.A.':'Votorantim Cimentos S.A.',
        'Intertechne Consultores S.A.':'Intertechne Consultores S.A.',
        'Banco Safra S/A':'Banco Safra',
        'Rima Industrial S/A':'Rima Industrial',
        'Engevix Engenharia S/A':'Engevix Eng.',
        'Carvalho Hosken S A Engenharia E Construcoes':'Carvalho Hosken',
        'Galvao Engenharia S/A':'Galvão Engenharia',
        'Mrv Engenharia E Participações Sa':'MRV',
        'Companhia Brasileira De Aluminio':'C. Bras. de Alumínio',
        'Tempo Serviços Ltda.':'Tempo Serviços',
        'Queiroz Galvao Alimentos Sa':'Queiroz Galvão Alim.',
        'Bradesco Leasing S/A Arrendamento Mercantil':'Bradesco Leasing',
        'Serveng Civiln S/A Empresas Associadas De Engenharia':'Serveng',
        'Bf Promotora De Vendas Ltda':'BF Promotora',
        'Odebrecht Oleo E Gas Sa':'Odebrecht Óleo e Gás',
        'Amil Assistencia Medica Internacional Sa':'Amil',
        'Companhia Brasileira De Metalurgia E Mineração':'C. Bras. De Metalurgia',
        'Banco Bmg':'Banco BMG',
        'Btg Pactual Asset Management Sa Distribuidora De Titulos E Valores Mobiliarios':'BTG Pactual Asset Mng.',
        'Via Engenharia S/A':'Via Eng.',
        'Brf S/A':'BRF',
        'Copersucar S/A':'Copersucar',
        'Vale Mina Do Sul S/A':'Vale Mina Do Sul',
        'Seara Alimentos Ltda':'Seara Alimentos',
        'Construtora Triunfo Sa':'Construtora Triunfo',
        'Arosuco Aromas E Sucos Ltda':'Arosuco',
        'Bradesco Capitalização Sa':'Bradesco Capitalização',
        'Recofarma Indústria Do Amazonas Ltda':'Recofarma',
        'Londrina Bebidas Ltda':'Londrina Bebidas',
        'Minerações Brasileiras Reunidas Sa - Mbr':'Min. Brasileiras Reunidas',
        'Vale Manganes S.A':'Vale Manganês',
        'Mineração Corumbaense Reunida S/A':'Mineração Corumbaense',
        'Sucocitrico Cutrale Ltda':'Cutrale',
        'Salobo Metais S/A':'Salobo Metais',
        'Banco Btg Pactual S/A':'BTG Pactual',
        'C R Almeida S/A Engenharia De Obras':'CR Almeida',
        'Carioca Christiani Nielsen Engenharia S/A':'Carioca Christiani Nielsen',
        'Oas Sa':'OAS',
        'Itau Unibanco Sa':'Itaú',
        'Bradesco Vida E Previdencia Sa':'Bradesco Previdência',
        'Braskem S/A.':'Braskem',
        'Flora Produtos De Higiene E Limpeza S/A':'Flora Higiene e Limpeza',
        'Vale Energia S/A':'Vale Enegria',
        'Arcelormittal Brasil S/A':'Arcelormittal',
        'Cosan Lubrificantes E Especialidades S.A.':'Cosan',
        'Construtora Norberto Odebrecht S/A':'Odebrecht',
        'Construtora Queiroz Galvao S.A':'Queiroz Galvão',
        'Crbs S.A.':'CRBS (Ambev)',
        'U T C Engenharia S/A':'UTC Engenharia',
        'Construtora Andrade Gutierrez  S.A.':'Andrade Gutierrez',
        'Construtora Oas Sa':'OAS',
        'Jbs S/A':'JBS'
    }
    
    return traducao[t] if t in traducao else t
    
def arrumaPF(p):
    traducao = {
        "pf":"Pessoas",
        "pj":"Empresas",
        "fundo": "Fundo Partidário"
    }
    return traducao[p]
    
def monta_json():
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/treemap2.csv")
    

    dados["cargo"] = dados["cargo"].fillna("Comitê ou Direção partidária")
    dados["doador"] = dados["doador"].apply(arrumaTitulo)
    dados["doador"] = dados["doador"].apply(traduz_doador)
    dados["receptor"] = dados["receptor"].apply(arrumaTitulo)
    dados["pf_pj"] = dados["pf_pj"].apply(arrumaPF)
    
    saida = {
        "name":"Doadores",
        "children": []
    }
        
    pf_pjs = set(dados["pf_pj"])
    lista_pf_pjs = []
    for p in pf_pjs:
        if p != "Fundo Partidário":
            temp = dados[dados.pf_pj == p]
            grandes = temp[temp.doador != "Outros"]
            doadores = set(grandes["doador"])
            lista_doadores = []
            for d in doadores:
                temp2 = temp[temp.doador == d]
                partidos = set(temp2["partido"])
                lista_partidos = []
                for s in partidos:
                    temp3 = temp2[temp2.partido == s]
                    cargos = set(temp3["cargo"])
                    lista_cargos = []
                    for c in cargos:
                        temp4 = temp3[temp3.cargo == c]
                        receptores = set(temp4["receptor"])
                        lista_receptores = []
                        for r in receptores:
                            uf = traduz_estado(list(temp4[temp4.receptor == r]["uf"])[0])
                            if str(r) == "Outros":
                                item = {"name":str(r), "value":int(sum(temp4[temp4.receptor == r]["valor"]))}
                            else:
                                item = {"name":str(r)+" ("+uf+")", "value":int(sum(temp4[temp4.receptor == r]["valor"]))}
                            lista_receptores.append(item)
                        item = {"name":str(c), "children":lista_receptores }
                        lista_cargos.append(item)
                    item = {"name":str(s), "children":lista_cargos }
                    lista_partidos.append(item)
                item = {"name":str(d), "children":lista_partidos }
                lista_doadores.append(item)
            
            grandes = {"name":"Grandes doadores", "children":lista_doadores }  
              
            outros = temp[temp.doador == "Outros"]
            temp2 = outros
            partidos = set(outros["partido"])
            lista_partidos = []
            for s in partidos:
                temp3 = temp2[temp2.partido == s]
                cargos = set(temp3["cargo"])
                lista_cargos = []
                for c in cargos:
                    temp4 = temp3[temp3.cargo == c]
                    receptores = set(temp4["receptor"])
                    lista_receptores = []
                    for r in receptores:
                        uf = traduz_estado(list(temp4[temp4.receptor == r]["uf"])[0])
                        if str(r) == "Outros":
                            item = {"name":str(r), "value":int(sum(temp4[temp4.receptor == r]["valor"]))}
                        else:
                            item = {"name":str(r)+" ("+uf+")", "value":int(sum(temp4[temp4.receptor == r]["valor"]))}
                        lista_receptores.append(item)
                    item = {"name":str(c), "children":lista_receptores }
                    lista_cargos.append(item)
                item = {"name":str(s), "children":lista_cargos }
                lista_partidos.append(item)
        
            outros = {"name":"Pequenos doadores", "children":lista_partidos }
        
            item = {"name":str(p), "children":[grandes]+[outros] }
            saida["children"].append(item)
        else:
            temp2 = dados[dados.pf_pj == p]
            partidos = set(temp2["partido"])
            lista_partidos = []
            for s in partidos:
                temp3 = temp2[temp2.partido == s]
                cargos = set(temp3["cargo"])
                lista_cargos = []
                for c in cargos:
                    temp4 = temp3[temp3.cargo == c]
                    receptores = set(temp4["receptor"])
                    lista_receptores = []
                    for r in receptores:
                        uf = traduz_estado(list(temp4[temp4.receptor == r]["uf"])[0])
                        if str(r) == "Outros":
                            item = {"name":str(r), "value":int(sum(temp4[temp4.receptor == r]["valor"]))}
                        else:
                            item = {"name":str(r)+" ("+uf+")", "value":int(sum(temp4[temp4.receptor == r]["valor"]))}
                        lista_receptores.append(item)
                    item = {"name":str(c), "children":lista_receptores }
                    lista_cargos.append(item)
                item = {"name":str(s), "children":lista_cargos }
                lista_partidos.append(item)
            
            item = {"name":str(p), "children":lista_partidos }  
            saida["children"].append(item)            
    
    print(saida)
    
    with open(path+'/doacoes2014.json', 'w', encoding='utf8') as outfile:
        json.dump(saida, outfile,ensure_ascii=False, sort_keys=True)

        

def conserta_json():
    path = os.path.dirname(os.path.abspath(__file__))
    with open(path+'/treemap.json',"r") as file:
        dados = json.load(file)
    
    print(dados)
        
def histogramas():
    path = os.path.dirname(os.path.abspath(__file__))
    dados = read_csv(path+"/doadores.csv")
    
    dados = dados.sort("valor",ascending=False)
    
    print(len(dados))
    x = 1
    valor = sum(dados["valor"][x-1 : x])
    outros = sum(dados["valor"][x:])
    while outros > valor:
        x +=1
        valor = sum(dados["valor"][x-1 : x])
        outros = sum(dados["valor"][x:])
        
        
    print(x)
    

def cria_json():
    #lê arquivo
    dados = faz_consulta("candidatos4")
    
    #cria coluna pra doação direta ou não e muda o doador_orig para o doador normal, se for o caso
    dados["direto"] = dados["doador_orig"].apply(lambda t: 0 if t == "" else 1)
    dados["doador"] = dados.apply(lambda t:t["doador_orig"] if t["doador_orig"] != "" else t["doador"],axis=1)
    
    #cria arquivo de saída
    arquivo = {"nodes":[],"links":[]}
    
    #cria um dicionário para guardar a posição de cada partido e empresa nos nodes
    indice = {}    
    
    #para cada empresa da lista
    empresas = ["JBS S/A","CRBS S/A"]
    for e in empresas:    
        #filtra empresa
        soh_jbs = dados[dados.doador == e]
    
        #tira colunas desnecessárias
        perguntas = ["doador","candidato","valor","partido","direto"]
        for p in soh_jbs.columns:
            if p not in perguntas:
                del soh_jbs[p]
    
        soh_jbs["valor"] = soh_jbs["valor"].apply(float)

    
        #agora, faz um processo diferente para os doadores diretos ou não
        diretos = soh_jbs[soh_jbs.direto == 0]
        indiretos = soh_jbs[soh_jbs.direto == 1]
    
        #para os diretos
        del diretos["direto"]
        #agrupa por candidato e soma o total
        saida = diretos.groupby("candidato").sum()
    
        #transforma esse arquivo somado em dicionário
        saida["candidato"] = saida.index
        dic_saida = saida.to_dict(outtype='records')
    
        #adiciona empresa ao arquivo de saída e no índice
        if e not in indice:
            indice[e] = len(arquivo["nodes"])
        arquivo["nodes"].append({"name":e,"group":1,"valor":sum(saida["valor"])})
    
        for v in dic_saida:
            i = len(arquivo["nodes"])
            arquivo["nodes"].append({"name":v["candidato"],"group":3})
            arquivo["links"].append({"source":i,"target":indice[e],"value":i*0.5})

        #agora para os indiretos
        del indiretos["direto"]
        #agrupa por candidato e soma o total
        saida2 = indiretos.groupby(["candidato","partido"]).sum()
    
        #transforma esse arquivo somado em dicionário
        saida2 = saida2.reset_index() 
        dic_saida2 = saida2.to_dict(outtype='records')
    
        #pega os partidos desta empresa
        partidos = list(set(saida2["partido"]))
    
        #enche o arquivo de saída com os partidos, checando no arquivo de índice se já há node para ele
        for p in partidos:
            if p not in indice:
                indice[p] = len(arquivo["nodes"])
                arquivo["nodes"].append({"name":p,"group":2,"valor":sum(saida["valor"])})
            arquivo["links"].append({"source":indice[p],"target":indice[e],"value":1})
        
        for v in dic_saida2:
            i = len(arquivo["nodes"])
            arquivo["nodes"].append({"name":v["candidato"],"group":3})
            arquivo["links"].append({"source":i,"target":indice[v["partido"]],"value":1})
    
        print(arquivo["nodes"])
        print(arquivo["links"])
    
    with open("jbs.json","w",encoding='utf8') as file:
        file.write(json.dumps(arquivo,ensure_ascii=False))

def arruma_doador(dados):
    #retira pessoas físicas e auto-doadores
    dados = dados[dados.cpf_cnpj != ""]
    dados["deletar"] = dados["cpf_cnpj"].apply(lambda t:1 if len(t.split("-")) > 1 else 0)
    dados = dados[dados.deletar == 0]
    del dados["deletar"]
    
    #troca nome dos doadores com nomes difernetes para o mesmo (o que aparece com maior valor)
    doadores = dados.groupby(["cpf_cnpj","doador"]).sum().sort("valor",ascending=False)
    doadores = doadores[doadores.valor > 100000]
    doadores = doadores.reset_index()
    del doadores["valor"]
 #   del doadores["direto"]
    traducao = {}
    for c in list(doadores["cpf_cnpj"]):
        if c not in traducao:
            traducao[c] = list(doadores[doadores.cpf_cnpj == c]["doador"])[0]
    dados['doador'] = dados.apply(lambda t:t["doador"] if t["cpf_cnpj"] not in traducao else traducao[t["cpf_cnpj"]],axis=1)
    
    #agora junta o nome das empresas do mesmo grupo
    traducao = {
        "BRADESCO VIDA E PREVIDENCIA S/A":"GRUPO BRADESCO",
        "FLORA PRODUTOS DE HIGIENE E LIMPEZA S.A.":"JBS S/A",
        "VALE ENERGIA S.A":"GRUPO VALE",
        "BRADESCO SAUDE S/A":"GRUPO BRADESCO",
        "MINERAÇÕES BRASILEIRAS REUNIDAS S/A":"GRUPO VALE",
        "MINERAÇÃO CORUMBAENSE REUNIDAS S.A.":"GRUPO VALE",
        "VALE MANGANES S.A.":"GRUPO VALE",
        "VALE MINA DO AZUL S.A":"GRUPO VALE",
        "AROSUCO AROMAS E SUCOS LTDA":"CRBS S/A (AMBEV)",
        "CRBS S/A":"CRBS S/A (AMBEV)",
        "BRADESCO LEASING S/A ARRENDAMENTO MERCANTIL":"GRUPO BRADESCO",
        "CONSTRUCOES E COMERCIO CAMARGO CORREA S.A.":"GRUPO CAMARGO CORREA",
        "CONSTRUTORA CAMARGO CORREA CONSTRUCOES INDUSTRIAIS S/A":"GRUPO CAMARGO CORREA",
        "BRADESCO S/A CORRETORA DE TITULOS E VALORES MOBILIARIOS":"GRUPO BRADESCO",
        "BRADESCO ADMINISTRADORA DE CONSORCIOS LTDA":"GRUPO BRADESCO",
        "BRADESCO CAPITALIZAÇÃO S/A":"GRUPO BRADESCO" ,
        "CRBS  (AMBEV)":"CRBS (AMBEV)"
    }
    
    dados["doador"] = dados["doador"].apply(lambda t:t if t not in traducao else traducao[t])
    dados["doador"] = dados["doador"].apply(lambda t:t.replace("S.A.","").replace("S/A","").replace("S.A","").replace(" SA","").strip())
    dados["doador"] = dados["doador"].apply(lambda t:t if t not in traducao else traducao[t])
        
    return dados
    
def cria_json2():
    #lê arquivo e filtra os deputados federais
    dados = faz_consulta("candidatos4")
    dados = dados[dados.cargo == "Deputado Federal"]
    
    #cria coluna pra doação direta ou não e muda o doador_orig para o doador normal, se for o caso
    dados["direto"] = dados["doador_orig"].apply(lambda t: 0 if t == "" else 1)
    dados["doador"] = dados.apply(lambda t:t["doador_orig"] if t["doador_orig"] != "" else t["doador"],axis=1)
    dados["cpf_cnpj"] = dados.apply(lambda t:t["cpf_cnpj_orig"] if t["cpf_cnpj_orig"] != "" else t["cpf_cnpj"],axis=1)
    
    #arruma o cnpj para tirar filiais e coloca valor em float
    dados["cpf_cnpj"] = dados["cpf_cnpj"].apply(lambda t:t.split("/")[0])
    dados["valor"] = dados["valor"].apply(float)
    
    #arruma nome de empresa
    dados = arruma_doador(dados)
    doadores = dados.groupby(["cpf_cnpj","doador"]).sum().sort("valor",ascending=False)

    #tira colunas desnecessárias
    perguntas = ["doador","candidato","valor","partido","direto","cpf_cnpj"]
    for p in dados.columns:
        if p not in perguntas:
            del dados[p]
            
    #cria arquivo de saída
    arquivo = {"nodes":[],"links":[]}
    
    #cria um dicionário para guardar a posição de cada partido e empresa nos nodes
    indice = {}        
    
    #para cada empresa da lista
    empresas = ["JBS", "CONSTRUTORA ANDRADE GUTIERREZ", "CONSTRUTORA OAS", "CRBS (AMBEV)", "CONSTRUTORA QUEIROZ GALVÃO", "GRUPO BRADESCO", "UTC ENGENHARIA", "CONSTRUTORA NORBERTO ODEBRECHT", "ITAU UNIBANCO", "AMIL ASSISTENCIA MEDICA INTERNACIONAL", "GALVÃO ENGENHARIA", "BRASKEM", "COSAN LUBRIFICANTES E ESPECIALIDADES", "CARIOCA CHRISTIANI NIELSEN ENGENHARIA", "BANCO BTG PACTUAL", "GRUPO VALE", "COMPANHIA METALURGICA PRADA", "CERVEJARIA PETROPOLIS", "ARCELORMITTAL BRASIL", "SAEPAR SERVICOS E PARTICIPACOES", "TELEMONT ENGENHARIA TELECOMUNICAÇÕES", "BRF", "SEARA ALIMENTOS LTDA", "COMPANHIA BRASILEIRA DE METALURGIA E MINERAÇÃO", "BANCO BMG", "RIO DE JANEIRO REFRESCOS LTDA", "SALOBO METAIS", "GERDAU AÇOS ESPECIAIS", "COPERSUCAR", "SERRANA EMPREENDIMENTOS E PARTICIPAÇOES", "RECOFARMA INDUSTRIA DO AMAZONAS LTDA", "INDUSTRIAS BRASILEIRAS DE ARTIGOS REFRATÁRIOS - IBAR - LTDA", "SUPERMERCADOS BH COM ALIM LTDA", "ELDORADO BRASIL CELULOSE", "VIGOR ALIMENTOS", "ENGEVIX ENGENHARIA", "PORTO SEGURO COMPANHIA DE SEGUROS GERAIS", "RIMA INDUSTRIAL", "GRUPO CAMARGO CORREA", "GUARANI", "BANCONTANDER (BRASIL)", "COMPANHIA SIDERURGICA VALE DO PINDARE", "SPAL INDUSTRIA BRASILEIRA DE BEBIDAS", "IGUATEMI EMPRESA DE SHOPPING CENTERS", "CR ALMEIDA ENGENHARIA DE OBRAS", "S A PAULISTA DE CONSTRUCOES E COMERCIO", "BRASIL TRADING LTDA", "CAÇAPAVA EMPREITADA DE LAVOR LTDA", "CONSTRUTORANCHES TRIPOLONI LTDA", "RECREIO B. H. VEICULOS"]
    
    #adicionas as empresas no indice de uma vez
    for e in empresas:
        indice[e] = len(arquivo["nodes"])
        sub = dados[dados.doador == e]
        arquivo["nodes"].append({"name":e,"group":1,"index":indice[e],"posicao":indice[e],"valor":sum(sub["valor"])})
        
    for e in empresas:    
        #filtra empresa
        diretos = dados[dados.doador == e]
        print(e)
        print(len(diretos))
        del diretos["direto"]
        
        #agrupa por candidato e soma o total
        saida = diretos.groupby("candidato").sum()

        #transforma esse arquivo somado em dicionário
        saida["candidato"] = saida.index
        dic_saida = saida.to_dict(outtype='records')
        
        for v in dic_saida:
            i = len(arquivo["nodes"])
            if v["candidato"] not in indice:
                indice[v["candidato"]] = i
                arquivo["nodes"].append({"name":conserta_nome(v["candidato"]),"posicao":i,"group":3,"index":i,"partido":list(diretos[diretos.candidato == v["candidato"]]["partido"])[0]})
            arquivo["links"].append({"source":indice[v["candidato"]],"target":indice[e],"value":1,"empresa":e,"candidato":conserta_nome(v["candidato"])})
    
    with open("jbs.json","w",encoding='utf8') as file:
        file.write(json.dumps(arquivo,ensure_ascii=False))

def calcula_doador():
    #lê arquivo e filtra os deputados federais
    dados = faz_consulta("candidatos3")
    sqs = list(set(dados["sq"]))
    sqs = Series(sqs)
    sqs.to_csv("sqs.csv")
    dados = dados[dados.cargo == "Deputado Federal"]
    dados["valor"] = dados["valor"].apply(float)
    print(sum(dados["valor"]))
    
    #cria coluna pra doação direta ou não e muda o doador_orig para o doador normal, se for o caso
    dados["doador"] = dados.apply(lambda t:t["doador_orig"] if t["doador_orig"] != "" else t["doador"],axis=1)
    dados["cpf_cnpj"] = dados.apply(lambda t:t["cpf_cnpj_orig"] if t["cpf_cnpj_orig"] != "" else t["cpf_cnpj"],axis=1)
    
    #arruma o cnpj para tirar filiais e coloca valor em float
    dados["cpf_cnpj"] = dados["cpf_cnpj"].apply(lambda t:t.split("/")[0])
    
    #arruma nome de empresa
    dados = arruma_doador(dados)
    doadores = dados.groupby(["cpf_cnpj","doador"]).sum().sort("valor",ascending=False)
    doadores = doadores.reset_index()
    print(doadores)
        
    doadores.to_csv("burgallops.csv",index=False)
    candidatos.to_csv("burghey.csv",index=False)
    

def conserta_nome(s):
    s = s.title()
    s = s.replace(" Do "," do ").replace(" De "," de ").replace(" Da "," da ")
    return s

#calcula_doador()
#cria_json2()
#print("oi")
#checa_despesas()
#faz_consulta("candidatos3")
#faz_consulta("comites3")
#roda_candidatos()
le_candidatos()
#roda_comites()
#print(faz_consulta("comite_cnpj"))
#faz_consulta("comite_cnpj")
#faz_consulta("candidato_cnpj")
#faz_consulta("desp_candidatos3")
#print(acha_comites())
#compara_arquivos()
#despesa_candidatos()
#despesa_comites()
#uniformiza_cnpj()
#comite_cnpj()
#candidato_cnpj()
#acha_cnpjs()
#totaliza_doacoes()
#compara_doacoes()
#compara2()
#monta_json()
#conserta_json()
#histogramas()
#uniformiza_cnpj()
