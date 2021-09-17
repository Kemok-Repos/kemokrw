from sqlalchemy import create_engine
import datetime
import pytz

class DbClient(object):
    def __init__(self,oid, db):
        self.engine = create_engine(db)
        self.inicio = datetime.datetime.now(pytz.timezone('America/Guatemala'))
        self.oid = oid


    def ejecutar(self,sql,tipo='fetchone'):
        conn = self.engine.connect()
        rs = conn.execute(sql)
        if tipo == 'fetchall':
            total = rs.fetchall()
        elif tipo == 'fetchone':
            total = rs.fetchone()

        conn.close()
        if tipo == 'fetchall' or tipo == 'fetchone':
            return total


    def get_maestro_origen(self, id):
        sql = 'select origen, descripcion, credenciales, propiedades from maestro_de_origen where id={}'.format(id)
        total = self.ejecutar(sql)
        return total[0], total[1], total[2], total[3]


    def get_maestro_verificacion(self):
        sql = 'select id, name, id_tabla_origen, id_tabla_destino from maestro_de_verificacion'
        total = self.ejecutar(sql, tipo='fetchall')
        return total


    def get_maestro_tabla(self,id):
        sql = 'select id_origen, nombre, verificacion from maestro_de_tablas ' \
              ' where id={} '.format(id)
        total = self.ejecutar(sql)
        return total[0], total[1], total[2]


    def BuscarTable(self,Datbase_id, OldTable_id):
        sql ="SELECT id FROM public.metabase_table where db_id ="+\
             str(Datbase_id)+" and name in (select name from public.metabase_table " \
                             "where id="+str(OldTable_id)+")"

        conn = self.engine.connect()
        rs = conn.execute(sql)
        total = rs.fetchone()[0]
        conn.close()
        return total


    def logger(self, nombre_de_accion, nivel, tipo, detalle):
        self.fin = datetime.datetime.now(pytz.timezone('America/Guatemala'))
        dictstr = str(detalle).replace("'", "\"")
        sql = "INSERT INTO maestro_de_logs (oid, nombre_de_accion, nivel, tipo, fecha_de_inicio, fecha_de_fin, detalle)"
        sql += "VALUES ('%s','%s','%s','%s','%s','%s','%s')" % (str(self.oid), nombre_de_accion, nivel, tipo,
                                                                str(self.inicio), str(self.fin), dictstr)
        conn = self.engine.connect()
        conn.execute(sql)
        conn.close()

