# Kredential

Módulo python el cual que permite mostarr las credenciales de los recursos almacenados en passbolt para un usuario en particular.

Esta implementado mediante la api de  passbolt y pone a disposicion del usuario 4 métodos

**discover_full()** 
muestra una lista completa con los datos en passbolt, las clave que retorna por cada recurso encotnrado en el perfil del usuario son: 
(id,name,uri,user,password,description)
                  
**dicover_credId(id)**
 retorna en formato json las credenciales almacenadas en passbbolt para el id dado.

**discover_credResource(resource_name)**
 retorna en formato json las credenciales almacenadas en passbbolt para el resource_name dado.

1) Bajar el modulo kredential en kredential.zip.
2) Descomprimir carpeta
3) cd  kredential
3) Configura ambiente de trabajo python > =3,6
4) pip install requerimient.txt
5) cd  kredential
6) editar archivo config.ini:
   copie en el perfil del usuario passbolt con el cual se conectara kredential, la clave pública y privada.
   en los archivos public.asc y private.asc.
   
   
[PASSBOLT] ****
SERVER = http://passbolt.kemok.tech/

USER_FINGERPRINT=
 
USER_PUBLIC_KEY_FILE=public.asc

USER_PRIVATE_KEY_FILE=private.asc

PASSPHRASE=


Eestos datos los encontrara en la sección keys inpector del portal de passbolt, en el caso de USER_FINGERPRINT copi el literal y peguelo en el archtivo config.ini. Para el caso de public.asc y private.asc debe construir este archivo con las claves publicas y privadas, respectivamente; esta información también la encontrar en el keys inspector.

7) registrar claves cargadas en archivos public.asc y private.asc:

         $ gpg --import public.asc
         $ gpg --import  --batch   private.asc  

para verificar si las claves estan registras
        $ gpg --list-key

Ejemplo ejecución:

     $ source env/bin/activate
     
     $ python 
     
    >>> from kredential.kredential import discover_credResource,discover_full,discover_credId,json_to_sqlalchemy
                
    >>> credenciales = discover_full()  
                          
    >>> credencial = discover_credResource(“nombre_del_resource”)
                 
    >>> credencial = discover_credId(“id_recource passbolt”)
       
    >>> string_connect = json_to_sqlalchemy(credenciales)
        