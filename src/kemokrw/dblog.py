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


