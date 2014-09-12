import numpy as np
from pandas import DataFrame, read_csv, Series, pivot_table
from pymongo import MongoClient
import urllib.parse, urllib.request
import math
from urllib.request import urlopen
from bs4 import BeautifulSoup
import time
from functools import wraps
import errno
import os
import signal
import re
import hashlib

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
    colecao = conecta("candidatos2")
    antigas = doacoes_antigas("candidatos2")
 #   colecao.remove()
    candidatos = le_candidatos()
    codigos = [str(c) for c in list(candidatos["sequencial"])]

    for c in codigos:
        c = str(c)
        url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoReceitasByCandidato.action?sqCandidato='+c+'&tipoEntrega=2'
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
   colecao = conecta("desp_candidatos2")
   antigas = doacoes_antigas("desp_candidatos2")
   candidatos = le_candidatos()
   codigos = [str(c) for c in list(candidatos["sequencial"])]
   dados_dict = candidatos.set_index('sequencial').to_dict()
   for c in codigos:
        c = str(c)
        url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoDespesasByCandidato.action?sqCandidato='+c+'&tipoEntrega=2'
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
   colecao = conecta("desp_comites2")
   antigas = doacoes_antigas("desp_comites2")
   comites = le_comites()
   codigos = [str(c) for c in list(comites["codigo"])]
   dados_dict = comites.set_index('codigo').to_dict()
   for c in codigos:
       c = str(c)
       url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoDespesasByComite.action?sqComiteFinanceiro='+c+'&tipoEntrega=2'
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
    colecao = conecta("comites2")
    antigas = doacoes_antigas("comites2")
    comites = le_comites()
    codigos = [str(c) for c in list(comites["codigo"])]
    for c in codigos:
        c = str(c)
        url = 'http://inter01.tse.jus.br/spceweb.consulta.receitasdespesas2014/resumoReceitasByComite.action?sqComiteFinanceiro='+c+'&tipoEntrega=2'
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
    dados = read_csv(path+"/numeros_candidatos.csv")
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

    candidatos = read_csv(path+"/candidatos.csv")
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
            print(c)
            temp = candidatos[candidatos.cpf_cnpj == c]
            lista = list(temp.groupby("doador").agg({"recibo":Series.nunique}).sort("recibo",ascending=0).index)
            candidatos["doador"][candidatos.cpf_cnpj == c] = candidatos[candidatos.cpf_cnpj == c]["doador"].apply(lambda t:lista[0])
    
    candidatos.to_csv(path+"/candidatos3.csv")
    
    
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
    dados = read_csv(path+"/doacoes_burga_limas.csv")
    dados["pf_pj"] = dados["cnpj"].apply(lambda t:"pj" if "/" in t else "pf")
    dados["valor"] = dados["valor"].apply(lambda t:float(t.replace(",",".")))
    empresas = ["JBS S/A","CONSTRUTORA OAS SA","CONSTRUTORA ANDRADE GUTIERREZ  S.A.","U T C ENGENHARIA S/A","CRBS S.A.","CONSTRUTORA QUEIROZ GALVAO S.A","CONSTRUTORA NORBERTO ODEBRECHT S/A","COSAN LUBRIFICANTES E ESPECIALIDADES S.A.","ARCELORMITTAL BRASIL S/A"]
    dados = dados[dados.doador.isin(empresas)]
    tabela = pivot_table(dados, values="valor",index=["doador","partido"], aggfunc=np.sum)
    tabela.to_csv(path+"/tabela.csv")
    print(tabela)
    print(sum(tabela))
    
#checa_despesas()
#print(faz_consulta("despesa_candidatos"))
#roda_candidatos()
#roda_comites()
#print(faz_consulta("comite_cnpj"))
#faz_consulta("candidato_cnpj")
#faz_consulta("desp_comites2")
#faz_consulta("desp_candidatos2")
#print(acha_comites())
#compara_arquivos()
#despesa_comites()
#despesa_candidatos()
#uniformiza_cnpj()
#comite_cnpj()
#candidato_cnpj()
#acha_cnpjs()
totaliza_doacoes()