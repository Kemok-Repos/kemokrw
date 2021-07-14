from sqlalchemy import create_engine
import pytz
import datetime
import json

class DbLog(object):
	def __init__(self,oid,db):
		#db="postgresql://katadmin:$q%$=0#WyCI1.@localhost/kat"
		self.engine = create_engine(db)
		self.inicio = datetime.datetime.now(pytz.timezone('America/Guatemala'))
		print(self.inicio)
		self.fin = None
		self.oid = oid


	def logger(self,nombre_de_accion,nivel,tipo,detalle):
		self.fin =datetime.datetime.now(pytz.timezone('America/Guatemala'))
		dictstr =str(detalle).replace("'", "\"")
		sql = "INSERT INTO maestro_de_logs (oid, nombre_de_accion, nivel, tipo, fecha_de_inicio, fecha_de_fin, detalle)"
		sql += "VALUES ('%s','%s','%s','%s','%s','%s','%s')" % (str(self.oid), nombre_de_accion, nivel, tipo,
																str(self.inicio), str(self.fin), dictstr)
		conn = self.engine.connect()
		conn.execute(sql)
		conn.close()


	def UpdateMaestroGsheet(self,nombre_de_accion,status):
		self.fin =datetime.datetime.now(pytz.timezone('America/Guatemala'))
		sql = "UPDATE public.maestro_de_gsheetdb set estatus ='{}' ".format(status)
		sql += "WHERE nombre_de_accion like '{}'".format("%%")
		conn = self.engine.connect()
		try:
			conn.execute(sql)
		except Exception as e:
			print(str(e))
		try:
			conn.close()
		except Exception as e:
			print(str(e))


	def get_ServiceStatus(self):
		sql = "SELECT estatus FROM maestro_de_gsheetdb limit 1"
		conn = self.engine.connect()
		rs = conn.execute(sql)
		total = rs.fetchone()[0]
		conn.close()
		return total


	def DbConections(self, dbcode):
		# será implementada mediante passbolt

		dbConection = []
		pwd_gsheet = '7ba40379e4597bf535dfa79f9c45b60a'
		pwd_gsheet2 = 'jbfdbi%%ms.$2773lnnwn'

		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@50.116.33.86/panamacompra')
		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@45.56.113.157/guatecompras')
		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@45.79.204.111/bago')
		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@45.79.204.111/bago_caricam')
		dbConection.append('postgresql://notificaciones_marketing:'
						   '7ba40379e4597bf535dfa79f9c45b60a'
						   '@192.155.95.216/notificaciones_marketing')
		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@bacgt.cg9u5bhsoxjc.us-east-1.rds.amazonaws.com/bacgt')
		dbConection.append('postgresql://g2sheets:' + pwd_gsheet2
						   + '@172.105.156.208/aquasistemas')
		dbConection.append('postgresql://forge:68qCIg1PMdOHOkC09qsE@96.126.123.195:5432/expolandivar2021')
		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@45.79.216.118/srtendero')

		dbConection.append('postgresql://g2sheets:' + pwd_gsheet
						   + '@45.79.9.70/guatecompras2')

		# db pruebas
		dbConection.append('postgresql://admin:admin@localhost:5433/panamacompra')
		dbConection.append('postgresql://admin:admin@localhost:5433/guatecompras')
		dbConection.append('postgresql://admin:admin@localhost:5433/bago')
		dbConection.append('postgresql://admin:admin@localhost:5433/bago_caricam')
		dbConection.append('postgresql://notificaciones_marketing:'
						   '7ba40379e4597bf535dfa79f9c45b60a'
						   '@192.155.95.216/notificaciones_marketing')

		return dbConection[dbcode]