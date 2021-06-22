# kemok-rw

kemok-rw es una librería desarrollada para leer y escribir información desde distintos tipos de fuentes de datos y a traves de diferentes algoritmos de transferencia.

## Objetivo

Crear una libreria de transferencia de información que sea eficiente, confiable y segura.

## Instalación

pip install git+https://github.com/Kemok-Repos/kemokrw.git

Se provee un documento de requerimientos para levantar un virtualenv.

## Para realizar el build

https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives

## ¿Cómo usar?

La librería esta diseñada para usarse instanciando 3 objetos:

- Un objeto de extracción
- Un objeto de carga
- Un objeto de transferencia

La transferencia se lleva a cabo cuando el objeto de extracción y el objeto de carga tienen columnas compatibles en número y en tipo. 

Cada objeto se debe instanciar siguiendo las instrucciones propias de la subclase.

### ExtractDB

Esta clase tiene como proposito ser una interfaz entre una base de datos de origen y un algoritmo de transferenicia. Actualmente tiene como soporte las bases de datos en:

- PostgreSQL
- SQL Server

Para instaciar un objeto es necesario brindarles los siguientes parametros:

- db : Un texto que contenga las credenciales de en formato "connection string" de SQLAlchemy. [Ver referencia.][1]
- table : El nombre de la tabla, vista o vista materializada de donde se obtendran los datos.
- model : Un diccionario de la información de cada columna asignada a la llave correspondiente (col1, col2, col3 ...) en donde se detalle el nombre (name) y el tipo de columnas ("type")
- condition (opcional) :  La condición que filtra la información a extraer.
- order (opcional) : Las columnas por las que se ordenara el query de selección.

### LoadDB

Esta clase tiene como proposito ser una interfaz entre una base de datos de destino y un algoritmo de transferenicia. Actualmente tiene como soporte las bases de datos en:

- PostgreSQL
- SQL Server

Para instaciar un objeto es necesario brindarles los siguientes parametros:

- db : Un texto que contenga las credenciales de en formato "connection string" de SQLAlchemy. [Ver referencia.][1]
- table : El nombre de la tabla, vista o vista materializada de donde se obtendran los datos.
- model : Un diccionario de la información de cada columna asignada a la llave correspondiente (col1, col2, col3 ...) en donde se detalle el nombre (name) y el tipo de columnas ("type")
- condition (opcional) :  La condición que filtra la información a extraer.
- order (opcional) : Las columnas por las que se ordenara el query de selección.

### BasicTransfer

Esta clase tiene como proposito realizar una transferenicia sencilla siguiento el siguiente algoritmo.

1. Verificación de compatibilidad
2. Verificación de checkeos. En caso la información en el origen es igual al destino se finaliza.
3. Transferencia sencilla origen->destino
4. Verificación de checkeos.

[1]: https://docs.sqlalchemy.org/en/14/core/engines.html    
