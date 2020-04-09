#########################################################
#
#	TODO:
#
#		OPEN/CLOSE CONNECTIONS? (in XXX server)
#		**** USE A CONNECTION POOL? ****
#
#		CONFIG FILE? (in XXX server)
#
#
#########################################################

from flask import Flask
from flask_cors import CORS
from flask import request

import mysql.connector

import json
import numpy

import configparser 
from pathlib import Path

from requests.auth import HTTPBasicAuth
import requests

import module.iXXXclassifiers
import geocode.geocode as geocode
import geogoogle.geogoogle as geogoogle
import scraping.scraping as scraping
import duplication.duplicated as duplication
import google_places.googleplaces as googleplaces
import nifservice.nifservice as nifservice


import logging

import operator



from difflib import SequenceMatcher

# keys and passwords
# make sure config.py is in .gitignore
from config import *

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, send_wildcard=True)



#mycursor = None
#mydb = None
#CONFIG_FILE = r'config.cfg'

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)



#########################################
#	ACTIVIDADES - only first level
#########################################
#categorias = ['IX','Z','79','FS','X','A','AA','I','II','III','IV','V','VI','VII','F','FP','VIII','85']
ACTIVIDADES = {
	'I': 'Produção Primária',
	'I': 'Produçao Primária',
	'II':'Indústria',	
	'III':'Restauração e bebidas',
	'III':'Restauração',
	'IV':'Grossistas',
	'V':'Retalho',
	'VI':'Estabelecimentos de venda directa',
	'VI':'Venda Directa',
	'VII':'Vendas à distância (por Catálogo e Internet)',
	'VII':'Vendas à distância',
	'VII':'Vendas à Distância',
	'VII':'Venda à Distância',
	'VIII':'Produção e Comércio',
	'VIII':'Produçao e Comércio',
	'IX':'Prestação de Serviços',
	'IX':'Prestaçao de Serviços',
	'X':'Segurança e Ambiente',
	'Z':'Sem actividade identificada',
	'Z':'s/ Actividade'
}





# given an activity (code or description) returns a string: 
# "code - description"
def getActivity(code_or_desc):
	if ACTIVIDADES.get(code_or_desc):
		return code_or_desc + ' - ' + ACTIVIDADES[code_or_desc]
	for code, desc in ACTIVIDADES.items():
		if SequenceMatcher(a=desc, b=code_or_desc).ratio() > 0.9:
			return code + ' - ' + desc
	return None


#########################################
#	DATABASES
#########################################

def MySQLConnect():
	mycursor = None
	mydb = None

	try:
		mydb = mysql.connector.connect(**MYSQL_CONN_INFO)			
		mycursor = mydb.cursor()		
		logger.info (">>>>>>>> ## CONNECTED TO MYSQL ##")
	except Exception as e:
		logger.error ("\n########################################")
		logger.error ("## ERROR - UNABLE TO CONNECT TO MYSQL ##")
		logger.error ("## check credentials and/or service   ##")
		logger.error ("########################################")	
		
	return mycursor, mydb




#########################################
#	CONNECTION TEST
#########################################
@app.route("/")
def hello():
    return "Hello World!"

@app.route("/test1", methods=['GET'])
def test1():
    return json.dumps({'status':'OK', 'error_code':0,'message':'test 1'})


@app.route("/test2", methods=['POST'])
def test2():
    return json.dumps({'status':'OK', 'error_code':0,'message':'test 2'})
    
    
@app.route("/test3", methods=['GET','POST'])
def test3():
    return json.dumps({'status':'OK', 'error_code':0,'message':'test 3'})
    

#########################################
#	GEOCODING
#########################################
@app.route('/geocode', methods=['POST'])
def getGeocodeData():
	services = [	
				{"id": "google", "service": "GOOGLE", "key": GEOCODE_KEYS['google']},
				{"id": "tomtom", "service": "TOMTOM", "key": GEOCODE_KEYS['tomtom']},				
				{"id": "here", "service": "HERE", "key": "", "app_id":GEOCODE_KEYS['here_id'],"app_code":GEOCODE_KEYS['here_code']},
				{"id": "bing", "service": "BING", "key": GEOCODE_KEYS['bing']},				
	]
	ignore = []

	try:
		city = None if not request.json['city'] else request.json['city']
		country = None if not request.json['country'] else request.json['country']
		geo = geocode.Geocode(services, ignore)
		results = geo.geocode(addr=request.json['addr'], local= city, country = country, saveraw = True)		
	except geocode.OutOfServices as e:
		results = {'status':'ERROR', 'error_code':4}
	except Exception as e:	
		results = {'status':'ERROR', 'error_code':5}
		#pass

	#logger.info (results)
	return json.dumps(results)



#########################################
#	SCRAPING I
#########################################
@app.route('/scraping', methods=['POST'])
def getScrapingData():
	s = scraping.Scrapping()
	results = {'status':'OK'}
	try:
		name = None if not request.json['name'] else request.json['name']
		nif = None if not request.json['nif'] else request.json['nif']
		results = s.scrap(name, nif)
		#logger.info (results)
	except Exception as e:
		logger.error (e)
		results = {'status':'ERROR', 'error_code':3}
	return json.dumps(results)


#########################################
#	GOOGLE GEOCODE + PLACE
#########################################
@app.route('/geogoogle', methods=['POST'])
def getGeoGoogleData():
	try:
		geo = geogoogle.Geogoogle(GEOCODE_KEYS['google'])	
		addr_name = None if not request.json['addr_name'] else request.json['addr_name']
		city = None if not request.json['city'] else request.json['city']
		country = None if not request.json['country'] else request.json['country']
		results = geo.getGeoPlaceInfo(addr_name,city,country) 
		#logger.info (results)	
	except Exception as e:
		logger.error (e)
		results = {'status':'ERROR', 'error_code':3}

	return json.dumps(results)



#########################################
#	GOOGLE PLACES
#########################################
@app.route('/googleradius', methods=['POST'])
def getGoogleRadius():	
	k_words = request.json['keywords'].split(',')
	try:
		results = googleplaces.getAllPlaces(
			key = GEOCODE_KEYS['google'], 
			latitude = float(request.json['latitude']), 
			longitude = float(request.json['longitude']), 
			radius = int(request.json['radius']), 
			type= request.json['types'],
			keywords=k_words,
			total = int(request.json['limite']))
		logger.info (results)	
	except Exception as e:
		logger.error (e)
		results = {'status':'ERROR', 'error_code':1}

	return json.dumps(results)


#########################################
#	NIF API
#########################################
@app.route('/nifservice', methods=['POST'])
def getNifData():	
	try:		
		results = nifservice.getNifInfo(nif=request.json['nif'], key=NIF_KEY)
		#logger.info (results)	
	except Exception as e:
		#logger.error (e)
		results = {'status':'ERROR', 'error_code':1}

	return json.dumps(results)



#########################################
#	MESSAGE ANALYSIS - CLASSIFIERS
#########################################


def classifier_Model2(message):
	"""
		Given a complain text, applies classifier 2 and
		returns a json with the results
		
		- actividade {I ... X, Z}
		- infraccao {Crime, Contraordenacao, Indefinido}
		- competencia simples {True, False}
	"""
	
	headers = {"Accept": "application/json"}
	auth = HTTPBasicAuth('admin', 'admin')
	resp1 = resp2 = resp3 = None
	response = {}
	
	try:		
		# get everything from model 1
		#response = module.iXXXclassifiers.classify(message)
		
		# now try to get list of activities from model 2
		
		# actividades
		resp1 = requests.post(
				CLASSIFIER_2_IP_1, 
				json={"text": message, "classifier": "LinearSVC"},
				headers=headers , 
				auth=auth, 
				timeout=CLASSIFIER_2_TIMEOUT)

		# competencia_simples
		resp2 = requests.post(
				CLASSIFIER_2_IP_2, 
				json={"text": message, "classifier": "LinearSVC"},
				headers=headers , 
				auth=auth, 
				timeout=CLASSIFIER_2_TIMEOUT)
		
		# infraccao
		resp3 = requests.post(
				CLASSIFIER_2_IP_3, 
				json={"text": message, "classifier": "LinearSVC"},
				headers=headers , 
				auth=auth, 
				timeout=CLASSIFIER_2_TIMEOUT)


		# order activities by probability
		if len(resp1.json()['probabilities']) != 0:
			x = resp1.json()['probabilities']
			sorted_x = sorted(x.items(), key=operator.itemgetter(1))
			acts = [i[0] for i in sorted_x][::-1]
			response['actividade'] = acts
		else:
			response['actividade'] = [None]
			
		# get the competencia_simples with the highest probability
		if len(resp2.json()['probabilities']) != 0:
			x = resp2.json()['probabilities']
			response['competencia_simples'] = [max(x.items(), key=operator.itemgetter(1))[0]]
		else:
			response['competencia_simples'] = [None]

		# get the infraccao with the highest probability
		if len(resp3.json()['probabilities']) != 0:
			x = resp3.json()['probabilities']
			response['infraccao'] = [max(x.items(), key=operator.itemgetter(1))[0]]
		else:
			response['infraccao'] = [None]


	except requests.ConnectionError as e:
		logger.error("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
		logger.error(str(e))
	except requests.Timeout as e:
		logger.error("OOPS!! Timeout Error")
		logger.error(str(e))
	except requests.RequestException as e:
		logger.error("OOPS!! General Error")
		logger.error(str(e))
	except KeyboardInterrupt:
		logger.error("Someone closed the program")
		logger.error(str(e))
	except Exception as e:
		logger.error("Failed to reach remote server.")
		logger.error(str(e))

	if response:		
		return response
	else:
		logger.error("Failed to fetch classification from remote server.")
		return []
	


def analyzeMsg(message, model = 1):
	"""
		Given the text of a complain and which model to use
		returns a 4-tuple of arrays: (actividades, competencia simple, competencia, infraccao)
	"""
	
	# the results are all in list format
	actividades_model = []
	comp_simples_model = []
	comp_model = []
	infraccao_model = []
	# class joao
	if model == 1:
		results = module.iXXXclassifiers.classify(message)
		for i in results['Categoria Actividade']:
			actividades_model.append(getActivity(i))
		for i in results['Competência Simples']:
			comp_simples_model.append(True if i==1 else False)
		for i in results['Competência']:
			comp_model.append(i)
		# model 1 does not have infraccao
		infraccao_model.append(None)
	else:
		# class luis
		results = classifier_Model2(message)
		for i in results['actividade']:
			actividades_model.append(getActivity(i))
		for i in results['competencia_simples']:
			comp_simples_model.append(i)	
		for i in results['infraccao']:
			infraccao_model.append(i)
		# model 2 does not have competencia only competencia_simples
		comp_model.append(None)
		
	
	logger.info(results)
	
	return (actividades_model, comp_simples_model, comp_model, infraccao_model)
			


@app.route('/analyzedenuncia', methods=['POST'])
def analyze():
	'''
		Given the text of a complain and which model to use
		returns a 4-tuple of arrays: (actividades, competencia simple, competencia, infraccao)
	'''
	try:
		actividades_model, comp_simples_model, comp_model, infraccao_model = analyzeMsg(request.json['denuncia'], request.json['model'])
		results = {
				'status':'OK',
				'comp_model_simples':comp_simples_model,
				'comp_model':comp_model,
				'actividades_model': actividades_model,
				'infraccao_model': infraccao_model
		}
	except Exception as e:
		logger.error (e)
		results = {'status':'ERROR', 'error_code': 2}
	return json.dumps(results)
	
	
@app.route('/getanalysis', methods=['POST'])
def get_javascript_data():
	'''
		Given the id of the complain and which model to use
		returns a 11-tuple, which includes the complainer and the target 
		identification
	'''
	mycursor,mydb = MySQLConnect()
	if not (mycursor and mydb):
		return json.dumps({'status':'ERROR', 'error_code': 10})
	
	
	query = '''
			SELECT
				D.EMAIL_CONTENT AS email_content,
				C.DESIGNACAO AS COMPETENCIA,
				ACT.DESIGNACAO AS ACTIVIDADE_DESIGNACAO,
				ENT1.NOME AS ENTIDADE_DENUNCIANTE,
				ENT2.NOME AS ENTIDADE_VISADA,
				ACT.CODIGO AS ACTIVIDADE_CODE,
				GetClassConteudoDenunciaStr(ID_DENUNCIA) AS CLASSIFICACAO_CONTEUDO
			FROM
				DENUNCIAS D
			LEFT JOIN COMPETENCIA C ON (C.ID_COMP = D.ID_COMPETENCIA)
			LEFT JOIN ENTIDADE ENT1 ON (ENT1.ID_ENTIDADE = D.ID_DENUNCIANTE)
			LEFT JOIN ENTIDADE ENT2 ON (ENT2.ID_ENTIDADE = D.ID_ENTIDADE_VISADA)
			LEFT JOIN CORRESP_ACTIVIDADES CA ON (CA.CORRESP_ID_CORRESP = D.ID_DENUNCIA)
			LEFT JOIN ACTIVIDADE ACT ON (ACT.ID_ACT = CA.ACT_ID_ACT)
			WHERE
				D.ID_DENUNCIA = ''' + str(request.json['g_id_denuncia']) +  ''' 
			LIMIT 1
			'''

	
	mycursor.execute(query)
	msg =  mycursor.fetchone()

	try:
		actividades_model, comp_simples_model, comp_model, infraccao_model = analyzeMsg(str(msg[0]), request.json['model'])

		results = {
					'status':'OK',
					'id_denuncia':request.json['g_id_denuncia'],
					'competencia':msg[1] if msg[1] else '',
					'remetente':msg[3] if msg[3] else '',
					'entidade_visada':msg[4] if msg[4] else '',
					'actividade_db':(msg[5] + ' - ' + msg[2]) if (msg[5] and msg[2]) else '', 
					'comp_model_simples':comp_simples_model,
					'comp_model':comp_model,
					'actividades_model': actividades_model,
					'nat_juridica':msg[6].split('+') if msg[6] else '',
					'infraccao_model': infraccao_model
					}
	except Exception as e:
		logger.error (e)
		results = {'status':'ERROR', 'error_code': 2}


	mycursor.close()
	
	return json.dumps(results)


#########################################
#	SERVER SHUTDOWN
#########################################
'''
@app.route('/shutdown', methods=['POST'])
def shutdown():
	if mydb:
		mydb.close()
	return 'Server shutting down...'
'''


#########################################
#	DUPLICATION
#########################################
@app.route('/duplicated', methods=['POST'])
def getDupData():
	mycursor,mydb = MySQLConnect()
	if not (mycursor and mydb):
		return json.dumps({'status':'ERROR', 'error_code': 10})	
	
	
	bOnlyNIF = True
	
	# only if no name and no nif
	# but only lat, lon
	bQueryOnlyCoords = True
	
	val = []
	
	query = '''
    SELECT 
		ID,
		NOME,
		MORADA,
		IS_PARENT,
		NIF,
		ENT_TYPE,
		LATITUDE,
		LONGITUDE		
    FROM 
        _temp_sani	
	'''
	name = ''
	if request.json['nome'] != '':
		query += ' WHERE NOME like concat(%s,"%")'
		name = duplication.sanitizeStr(request.json['nome'])	
		val.append(name[:int(request.json['n_char'])])
		bOnlyNIF = False
		bQueryOnlyCoords = False
		
	if request.json['nif'] != '':
		if len(val) > 0:
			query += ' and '
		else:
			query += ' WHERE '
		query += 'nif = %s'
		val.append(request.json['nif'])
		bQueryOnlyCoords = False
	
	if request.json['latitude'] != '':
		latitude = float(request.json['latitude'])
		#bOnlyNIF = False
	else:
		latitude = 0
		bQueryOnlyCoords = False
		
	if request.json['longitude'] != '':
		longitude = float(request.json['longitude'])
		#bOnlyNIF = False
	else:
		longitude = 0
		bQueryOnlyCoords = False
			
	if request.json['morada'] != '':
		morada = duplication.sanitizeStr(request.json['morada'], replace_abrv = True)
	else:
		morada = ''
		
		
	if bQueryOnlyCoords:
		query += " where CalcDistance(_temp_sani.LATITUDE, _temp_sani.LONGITUDE, " + str(latitude) + ", " + str(longitude)  + ") < " + str(request.json['dup_radius'])

	
	
	if int(request.json['dup_max_results']) > 0:
		query += " limit " + request.json['dup_max_results']
		

	results = []

	data_unique = {'name':name, 'address': morada, 'lat': latitude, 'lon': longitude}
		
	try:
		mycursor.execute(query,tuple(val))
		entidades_to_check =  mycursor.fetchall()
		
		for entidade in entidades_to_check:
			#logger.info (entidade)
			
			# no fields are available other than nif => if nif are the same
			# then both entities are potentially the same
			if bOnlyNIF or bQueryOnlyCoords:				
				results.append(
					{
						'id':entidade[0],
						'nome':entidade[1], 
						'morada':entidade[2],
						'is_pai':entidade[3],					
						'nif':entidade[4],
						'type':entidade[5] 
					}
				)
				continue
				
			# use dup module to check all fields
			data_dup = {'name':entidade[1], 'address': entidade[2], 'lat': float(entidade[6]), 'lon': float(entidade[7])}
			
			if request.json['is_pai'] != '' and entidade[3]:
				if request.json['is_pai'] != entidade[3]:
					continue
			
			if request.json['nif'] != '' and entidade[4]:
				if request.json['nif'] != entidade[4]:
					continue
			
			if morada != '' and entidade[2]:				
				if len(morada.split()) > 1 and len(entidade[2].split()) > 1:					
					dup = duplication.isDup(data_unique, data_dup, sanitize = False, max_radius = request.json['dup_radius'], min_ratio = request.json['dup_ratio'])
				else:
					dup = duplication.isDup(data_unique, data_dup, sanitize = False, check_addresses = False, max_radius = request.json['dup_radius'], min_ratio = request.json['dup_ratio'])
			else:
				dup = duplication.isDup(data_unique, data_dup, sanitize = False, check_addresses = False, max_radius = request.json['dup_radius'], min_ratio = request.json['dup_ratio'])
			
			
			if dup['DUPLICATED'] != 0:
				results.append(
					{
						'id':entidade[0],
						'nome':entidade[1], 
						'morada':entidade[2],
						'is_pai':entidade[3],					
						'nif':entidade[4],
						'type':entidade[5] 
					}
				)

	except Exception as e:
		logger.error (e)
		results = {'status':'ERROR', 'error_code': 1, 'error_message': e}
	
	mycursor.close()

	return json.dumps(results)


#########################################
#	MAIN
#########################################

# MAIN
if __name__ == "__main__":
	logger.info ("\nAttempting to start server ...")
	app.run(threaded = True)


