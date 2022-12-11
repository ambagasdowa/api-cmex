#!/bin/python3
import pyodbc
import urllib
import os
import sys
import subprocess

# importing element tree
# under the alias of ET
import xml.etree.ElementTree as ET
from rich import print

from datetime import datetime,date,tzinfo, timedelta
import time
from rich.progress import track
from rich.progress import Progress


from pycfdi_transform import CFDI40SAXHandler
from pycfdi_transform.formatters.cfdi40.efisco_corp_cfdi40_formatter import EfiscoCorpCFDI40Formatter
from pycfdi_transform.formatters.cfdi40.cda_cfdi40_formatter import CDACFDI40Formatter

from re import split, sub
#Zip
import zipfile
#md5
import hashlib

def camelize(string):
    return ''.join(a.capitalize() for a in split('([^a-zA-Z0-9])', string)
                   if a.isalnum())


def camel_case(s):
    s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])
# print(camel_case('JavaScript'))
# print(camel_case('Foo-Bar'))
# print(camel_case('foo_bar'))
# print(camel_case('--foo.bar'))
# print(camel_case('Foo-BAR'))
# print(camel_case('fooBAR'))
#print(camel_case('foo bar'))

## NOTE Config

config = {
    "db_connection":{
        "server":"10.8.0.235",
        "driver":"ODBC Driver 17 for SQL Server",
        "database":"sistemas",
        "user":"zam",
        "password":"lis",
    },
    "download_config":{
        "token":"5365d430-32dc-4f0a-8725-905aeb373c1b",
        "http_path":"transportescp.xsa.com.mx:9050/?/descargasCfdi",
        "download_path":"/tmp/",
        "dir_path":"gst_xml/",
        "filename":"cfdi_?.zip",
    },
    "service_params":{ #Partialy implemented ** Obligatorios e implementados
        "representacion":"XML", #** XML,PDF,ACUSE
        "pageSize":"100", #** [0-100] default 50
        "fecha":'2022-12-10', #** yyyy-mm-dd
        "fechaInicial":'?', # yyyy-mm-dd
        "fechaFinal":'?', # yyyy-mm-dd
        "serie":'', # yyyy-mm-dd
        "folioEspecifico":'', # yyyy-mm-dd
        "folioInicial":'', # yyyy-mm-dd
        "folioFinal":'', # yyyy-mm-dd
        "uuid":'', # 2d340db1-9c08-4c97-9ca8-676dc648094e
    }
}



print("[green]Connecting...[green]")

for i in track(range(2), description="Processing..."):
    time.sleep(1)  # Simulate work being done

try:
    cnxn = pyodbc.connect(
        'Trusted_Connection=no;DRIVER={'+config['db_connection']['driver']+'};SERVER='+config['db_connection']['server']+';DATABASE='+config['db_connection']['database']+';UID='+config['db_connection']['user']+';PWD='+config['db_connection']['password']
    )
except pyodbc.Error as e:
    # print("Error %d: %s" % (e.args[0], e.args[1]))
    print("Error {}: {}".format(e.args[0], e.args[1]))
    sys.exit(1)

# import pyodbc as db # forgot the imports
#conn.setdecoding(db.SQL_CHAR, encoding='latin1')
# conn.setencoding('latin1')

print("[blue]DB Connected...[blue]")

#idPerson = 2
cursor = cnxn.cursor()
#cursor.execute('SELECT id, xml FROM prueba WHERE id=?', (idPerson,))

# I'm the important line
cursor.fast_executemany = True


# NOTE Insert Example
# sql = "insert into TableName (Col1, Col2, Col3) values (?, ?, ?)"
# tuples=[('foo','bar', 'ham'), ('hoo','far', 'bam')]
# cursor.executemany(sql, tuples)
# cursor.commit()
# cursor.close()
# connection.close()


# working example
#cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
# for row in cursor.fetchall():
#    print(row)

# Passing the path of the
# xml document to enable the
# parsing process
# parsing as xargs

print ("[violet]"+"Downloading files "+"[violet]")
# NOTE command for download
# https -vvv -d transportescp.xsa.com.mx:9050/5365d430-32dc-4f0a-8725-905aeb373c1b/descargasCfdi representacion==XML pageSize==50


for i in track(range(1), description="Cleaning storing dir ..."):    time.sleep(1)  # Simulate work being done
tmp_path = config['download_config']['download_path'] + config['download_config']['dir_path']

clean_dir_files = subprocess.run(["rm", "-r",tmp_path], stdout=subprocess.DEVNULL)

make_dir_files = subprocess.run(["mkdir", "-p",tmp_path+"pack",tmp_path+"unpack"], stdout=subprocess.DEVNULL)

cmex_token =config['download_config']['token']

http_path = config['download_config']['http_path'].replace('?',cmex_token)
download_path = config['download_config']['download_path']
dir_path =  config['download_config']['dir_path']
filename = config['download_config']['filename'].replace('?', str(int(time.time()))
)

pack = download_path+dir_path+"pack/"
unpack = download_path+dir_path+"unpack/"



for i in track(range(1), description="Downloading xml files ..."):
    time.sleep(1)  # Simulate work being done

#Variable arguments
# fechas , folios
#if fecha is None:
#fecha = config['service_params']['fecha'].replace('?',str(date.today()))
fecha = config['service_params']['fecha']
#if pageSize is None:
pageSize = config['service_params']['pageSize']
#if representacion is None:
representacion = config['service_params']['representacion']

download_files = subprocess.run([ "https" , "-vvv","--download" , http_path ,'representacion=='+representacion,'pageSize=='+pageSize,"fecha=="+fecha, "--output" , pack+filename ]) 


with zipfile.ZipFile(pack+filename, 'r') as zip_ref:
    zip_ref.extractall(unpack)

#list_files = subprocess.run(["ls","-1v","/tmp/gst_xml/unpack"], stdout=subprocess.DEVNULL)

# One with have the files 
def get_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

files = []
for file in get_files(unpack):
    files.append(file)

for i in track(range(1), description="unzipping and process files ..."):
    time.sleep(1)  # Simulate work being done
print (files)



# FILES
# this can be /tmp/dir 
#files_basename = '/home/ambagasdowa/github/Cmex/api-cmex/files/'

#files = [
#    'F7549E3A-4EAF-48C1-9F40-C701A4D0CCB0_FAC_46_11052022_014243.xml', 'D9F5AE04-485B-4110-8D0C-58E53D335167_FAC_49_11052022_070606.xml'
#]

#build the function from hir

for filename in files:
    source = unpack + filename
    print("[blue]source file : " + source + "[blue]")

    #hashlib.md5("example string").hexdigest()
# Open,close, read file and calculate MD5 on its contents 
    with open(source, 'rb') as file_to_check:
        # read contents of the file
        data = file_to_check.read()
        # pipe contents of the file through
        md5_returned = hashlib.md5(data).hexdigest()

    name, ext = os.path.splitext(filename)
    #uuid,doctype:FAC,idfac,Date,SomeCtrlnum
    split_data = str(name).split('_')
    print(split_data[0])
    print ("[red] MD5 sum : "+md5_returned+"[red]")

    save_file = (split_data[1]+'_'+split_data[2],split_data[0],md5_returned,datetime.now().isoformat(timespec='seconds'),'',1,
)

    qry_md5 = "select [_md5sum] from sistemas.dbo.cmex_api_controls_files where [_md5sum] = ?"
    md5 = False
    print("[yellow] before md5 found : "+str(md5)+"[yellow]")
    cursor.execute(qry_md5,(md5_returned,))
    for row in cursor.fetchall():
        print(row[0])
        if(row[0] == md5_returned):
            md5 = True
    print("[blue] md5 found : "+str(md5)+"[blue]")

    if(md5 != True):
        print("[blue] save file : "+str(source)+"[blue]")
        insert_file = 'insert into sistemas.dbo.cmex_api_controls_files \
        (labelname,_filename,_md5sum,created,modified,_status) values( \
        ?,?,?,?,?,? \
        )'
        for i in track(range(2), description="Saving to Complement data to database..."):
            time.sleep(1)  # Simulate work being done

        count = cursor.execute(insert_file, save_file)
        cursor.commit()

        # get last id from comprobante
        cursor.execute(
            "select IDENT_CURRENT('sistemas.dbo.cmex_api_controls_files') as id")

        cmex_api_controls_files_id = cursor.fetchone()[0]
        cursor.commit()
        print("[red]cmex_api_controls_files_id : "+str(cmex_api_controls_files_id)+"[red]")

#params = cmex_api_controls_files_id , source

        # NOTE after set the source start with the parsing :
        # First get the general information
        # path xml que queremos transformar
        path_xml = source
        print("[blue] Start CartaPorte20 DATA  XTRACTION [blue]")
        tree = ET.parse(source)
        # getting the parent tag of
        # the xml document
        root = tree.getroot()

        # https://docs.python.org/es/3.9/library/xml.etree.elementtree.html
        ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4',
              'cartapore20': 'http://www.sat.gob.mx/CartaPorte20'}


        transformer = CFDI40SAXHandler()  # Cfdi 4.0
        # transformer = CFDI40SAXHandler().use_concepts_cfdi40()  # Cfdi 4.0
        cfdi_data = transformer.transform_from_file(path_xml)

        # print("[blue] Overview of the CFDI [blue]")
        # print(cfdi_data)

        print("[blue] Start CFDI DATA XTRACTION [blue]")
        #print(cfdi_data['cfdi40'])
        #print(cfdi_data['cfdi40'].keys())
        #print(cfdi_data['cfdi40'].values())


        complements_items = ['version', 'serie', 'folio', 'fecha', 'no_certificado', 'subtotal', 'descuento', 'total', 'moneda', 'tipo_cambio', 'tipo_comprobante', 'metodo_pago', 'forma_pago',
                             'condiciones_pago', 'exportacion', 'lugar_expedicion', 'sello', 'certificado']

        concepts = ['confirmacion', 'emisor', 'receptor',
                    'conceptos', 'impuestos', 'complementos', 'addendas']


        # Use an extra comma in your tuples, and just join:

        # a = ((1,1,1),)
        # for i in range(2,10):
        #     a = a + ((i,i,i),)

        # Edit: Adapting juanpa.arrivillaga's comment, if you want to stick with a loop, this is the right solution:
        # a = [(1,1,1)]  # build a list
        # for i in range (2,10):
        #     a.append((i,i,i))  # append data to that list
        # a = tuple(a)   # convert the list into a tuple

        fields = ('cmex_api_controls_files_id',)
        save_complement = (cmex_api_controls_files_id, )

        # General info

        for ind, data in cfdi_data['cfdi40'].items():
            if ind in complements_items:
                #        print(ind)
                #        print(data)
                fields = fields + (ind,)
                save_complement = save_complement + (data,)

        # add finals rows
        #       cmex_api_standings_id       int null,
        #        cmex_api_parents_id         int null,
        #        created                     datetime null,
        #        modified                    datetime null,
        #        _status                     tinyint default 1 null

        cmex_api_standings_id = 1
        cmex_api_parents_id = 1
        created = datetime.now().isoformat(timespec='seconds')
        modified = ''
        status = 1

        add_fields = ['cmex_api_standings_id',
                      'cmex_api_parents_id', 'created', 'modified', '_status']
        add_save = [cmex_api_standings_id, cmex_api_parents_id,
                    created, modified, status]

        for this_tuple in add_save:
            #    print(this_tuple)
            save_complement = save_complement + (this_tuple,)

        for this_fields in add_fields:
            #    print(this_tuple)
            fields = fields + (this_fields,)


#        print("[blue] NewTuple [blue]")
#        print(save_complement)
#        print(fields)
#        print(type(save_complement))
#        print("Fields are : " + str(type(fields)))
#
#        print("Count of the element : " + str(len(save_complement)))
        # insert into db

        insert = "insert into sistemas.dbo.cmex_api_cfdi_comprobante(cmex_api_controls_files_id, version, serie, folio, fecha,no_certificado, subtotal, descuento, total, moneda,tipo_cambio, tipo_comprobante, metodo_pago, forma_pago ,condiciones_pago,exportacion, lugar_expedicion, sello, certificado ,cmex_api_standings_id,cmex_api_parents_id, created, modified, _status) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        # NOTE Working from hir
        for i in track(range(2), description="Saving to Complement data to database..."):
            time.sleep(1)  # Simulate work being done

        count = cursor.execute(insert, save_complement)
        cursor.commit()


        # get last id from comprobante
        cursor.execute(
            "select IDENT_CURRENT('sistemas.dbo.cmex_api_cfdi_comprobante') as id")
        comprobante_last_id = cursor.fetchone()[0]
        cursor.commit()
        print("[red]"+str(comprobante_last_id)+"[red]")

        element_qry = "insert into sistemas.dbo.cmex_api_cfdi_data( \
                        cmex_api_controls_files_id \
                        ,cmex_api_section_id \
                        ,cmex_api_tags_id \
                        ,value \
                        ,created \
                        ,modified \
                        ,_status \
                    ) values(?,?,?,?,?,?,?)"


        # === === === === === === === ===  Emisor === === === === === === === ===
        for i in track(range(2), description="Saving to Emisor data to database..."):
            time.sleep(1)  # Simulate work being done

        # Get the fields for emisor element
        element_id = 1  # Emisor
        emisor_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
        cursor.execute(emisor_element, (element_id,))

        elements = cursor.fetchall()
        for ids, ele in elements:
            print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")


        save_emisor = ()
        for emisor_id, name in elements:
            #    save_emisor = save_emisor + ((cmex_api_controls_files_id, element_id,emisor_id,cfdi_data['cfdi40']['emisor'][name],created,modified,status),)
            saved_emisor = (cmex_api_controls_files_id, element_id, emisor_id,
                            cfdi_data['cfdi40']['emisor'][name], created, modified, status,)
            cursor.execute(element_qry, saved_emisor)
            cursor.commit()


        # === === === === === === === ===  Receptor === === === === === === === ===

        for i in track(range(2), description="Saving to Receptor data to database..."):
            time.sleep(1)  # Simulate work being done

        # Get the fields for receptor element
        element_id = 2  # receptor

        receptor_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
        cursor.execute(receptor_element, (element_id,))

        elements = cursor.fetchall()

        for ids, ele in elements:
            print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

        save_receptor = ()
        for receptor_id, name in elements:
            #    save_receptor = save_receptor + ((cmex_api_controls_files_id, element_id,receptor_id,cfdi_data['cfdi40']['receptor'][name],created,modified,status),)
            saved_receptor = (cmex_api_controls_files_id, element_id, receptor_id,
                              cfdi_data['cfdi40']['receptor'][name], created, modified, status,)
            cursor.execute(element_qry, saved_receptor)
            cursor.commit()


        # === === === === === === === ===  Receptor === === === === === === === ===

        for i in track(range(2), description="Saving to impuestos data to database..."):
            time.sleep(1)  # Simulate work being done

        # Get the fields for impuestos element
        element_id = 4  # impuestos

        impuestos_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ? and id in (11,12)"
        cursor.execute(impuestos_element, (element_id,))

        elements = cursor.fetchall()

        for ids, ele in elements:
            print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

        save_impuestos = ()
        for impuestos_id, name in elements:
            #    save_impuestos = save_impuestos + ((cmex_api_controls_files_id, element_id,impuestos_id,cfdi_data['cfdi40']['impuestos'][name],created,modified,status),)
            saved_impuestos = (cmex_api_controls_files_id, element_id, impuestos_id,
                               cfdi_data['cfdi40']['impuestos'][name], created, modified, status,)
            cursor.execute(element_qry, saved_impuestos)
            cursor.commit()


        # === === === === === === === ===  cartaporte === === === === === === === ===
        # Retentions && tralations
        for i in track(range(2), description="Saving to retenciones data to database..."):
            time.sleep(1)  # Simulate work being done

        # Get the fields for retenciones element
        element_id = 5  # retenciones

        retenciones_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ? "
        cursor.execute(retenciones_element, (element_id,))

        elements = cursor.fetchall()

        for ids, ele in elements:
            print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

        save_retenciones = ()
        for retenciones_id, name in elements:
            #    save_retenciones = save_retenciones + ((cmex_api_controls_files_id, element_id,retenciones_id,cfdi_data['cfdi40']['retenciones'][name],created,modified,status),)
            saved_retenciones = (cmex_api_controls_files_id, element_id, retenciones_id,
                                 cfdi_data['cfdi40']['impuestos']['retenciones'][0][name], created, modified, status,)
            cursor.execute(element_qry, saved_retenciones)
            cursor.commit()


        # === === === === === === === ===  Traslados === === === === === === === ===
        # Retentions && tralations
        for i in track(range(2), description="Saving to traslados data to database..."):
            time.sleep(1)  # Simulate work being done

        # Get the fields for traslados element
        element_id = 6  # traslados

        traslados_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ? "
        cursor.execute(traslados_element, (element_id,))

        elements = cursor.fetchall()

        for ids, ele in elements:
            print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

        save_traslados = ()
        for traslados_id, name in elements:
            #    save_traslados = save_traslados + ((cmex_api_controls_files_id, element_id,traslados_id,cfdi_data['cfdi40']['traslados'][name],created,modified,status),)
            saved_traslados = (cmex_api_controls_files_id, element_id, traslados_id,
                               cfdi_data['cfdi40']['impuestos']['traslados'][0][name], created, modified, status,)
            cursor.execute(element_qry, saved_traslados)
            cursor.commit()


        # === === === === === === === ===  cartaporte === === === === === === === ===

        for i in track(range(2), description="Saving to cartaporte data to database..."):
            time.sleep(1)  # Simulate work being done

        # Get the fields for cartaporte element
        element_id = 7  # cartaporte

        cartaporte_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
        cursor.execute(cartaporte_element, (element_id,))

        elements = cursor.fetchall()

        for ids, ele in elements:
            print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

        print('[cyan]Go inside CartaPorte :[cyan]')
        for concept in tree.findall('.//cartapore20:CartaPorte', ns):
            print(concept.attrib)

        save_cartaporte = ()
        for cartaporte_id, name in elements:
            if( concept.attrib[camelize(name)] ) :
                saved_cartaporte = (cmex_api_controls_files_id, element_id, cartaporte_id,
                                    concept.attrib[camelize(name)], created, modified, status,)
                cursor.execute(element_qry, saved_cartaporte)
                cursor.commit()


        # === === === === === === === ===  ubicacion_origen === === === === === === === ===

        print('[cyan]Go inside ubicacion_origen :[cyan]')
        n = 0
        for concept in tree.findall('.//cartapore20:Ubicacion', ns):
            print(concept.attrib)

            # Get the fields for ubicacion_origen element
            print("[red] element_id : "+str(element_id)+"[red]")
            element_id = 8 + n  # ubicacion_origen
            n = n + 2
            ubicacion_origen_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(ubicacion_origen_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to ubicacion data to database..."):
                time.sleep(1)  # Simulate work being done

            save_ubicacion_origen = ()
            for ubicacion_origen_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_ubicacion_origen = (cmex_api_controls_files_id, element_id, ubicacion_origen_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_ubicacion_origen)
                    cursor.commit()



        # === === === === === === === ===  Domicilio  === === === === === === === ===

        print('[cyan]Go inside domicilio :[cyan]')
        n = 0
        for concept in tree.findall('.//cartapore20:Domicilio', ns):
            print(concept.attrib)

            # Get the fields for domicilio element
            print("[red] element_id : "+str(element_id)+"[red]")
            element_id = 9 + n  # domicilio
            n = n + 2
            domicilio_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(domicilio_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to ubicacion data to database..."):
                time.sleep(1)  # Simulate work being done

            save_domicilio = ()
            for domicilio_id, name in elements:


                if camelize(name) in concept.attrib  :
                    saved_domicilio = (cmex_api_controls_files_id, element_id, domicilio_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_domicilio)
                    cursor.commit()



        # === === === === === === === ===  Mercancias  === === === === === === === ===

        print('[cyan]Go inside mercancias :[cyan]')
        init = 12
        offset = 0
        step = 0
        for concept in tree.findall('.//cartapore20:Mercancias', ns):
            print(concept.attrib)

            # Get the fields for mercancias element
            element_id = init + offset  # mercancias
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            mercancias_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(mercancias_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to ubicacion data to database..."):
                time.sleep(1)  # Simulate work being done

            save_mercancias = ()
            for mercancias_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_mercancias = (cmex_api_controls_files_id, element_id, mercancias_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_mercancias)
                    cursor.commit()


        # === === === === === === === ===  mercancia  === === === === === === === ===

        print('[cyan]Go inside mercancia :[cyan]')
        init = 13
        offset = 0
        step = 0
        for concept in tree.findall('.//cartapore20:Mercancia', ns):
            print(concept.attrib)

            # Get the fields for mercancia element
            element_id = init + offset  # mercancia
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            mercancia_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(mercancia_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to ubicacion data to database..."):
                time.sleep(1)  # Simulate work being done

            save_mercancia = ()
            for mercancia_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_mercancia = (cmex_api_controls_files_id, element_id, mercancia_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_mercancia)
                    cursor.commit()

        # === === === === === === === ===  autotransporte  === === === === === === === ===

        print('[cyan]Go inside autotransporte :[cyan]')
        init = 14
        offset = 0
        step = 0
        for concept in tree.findall('.//cartapore20:Autotransporte', ns):
            print(concept.attrib)

            # Get the fields for autotransporte element
            element_id = init + offset  # autotransporte
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            autotransporte_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(autotransporte_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to ubicacion data to database..."):
                time.sleep(1)  # Simulate work being done

            save_autotransporte = ()
            for autotransporte_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_autotransporte = (cmex_api_controls_files_id, element_id, autotransporte_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_autotransporte)
                    cursor.commit()


        # === === === === === === === ===  identificacion  === === === === === === === ===

        init = 15
        offset = 0
        step = 0
        namespace = "identificacion_vehicular"

        print('[cyan]Go inside '+camelize(namespace)+' :[cyan]')
        for concept in tree.findall('.//cartapore20:'+ camelize(namespace) , ns):
            print(concept.attrib)

            # Get the fields for identificacion element
            element_id = init + offset  # identificacion
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            query_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(query_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to "+camelize(namespace)+" data to database..."):
                time.sleep(1)  # Simulate work being done

            save_query = ()
            for query_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_query = (cmex_api_controls_files_id, element_id, query_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_query)
                    cursor.commit()



        # === === === === === === === ===  identificacion  === === === === === === === ===

        init = 16
        offset = 0
        step = 0
        namespace = "seguros"

        print('[cyan]Go inside '+camelize(namespace)+' :[cyan]')
        for concept in tree.findall('.//cartapore20:'+ camelize(namespace) , ns):
            print(concept.attrib)

            # Get the fields for elements
            element_id = init + offset
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            query_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(query_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to "+camelize(namespace)+" data to database..."):
                time.sleep(1)  # Simulate work being done

            save_query = ()
            for query_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_query = (cmex_api_controls_files_id, element_id, query_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_query)
                    cursor.commit()




        # === === === === === === === ===  remolques  === === === === === === === ===

        init = 17
        offset = 0 # the normal is zero
        step = 0
        namespace = "remolque"

        print('[cyan]Go inside '+camelize(namespace)+' :[cyan]')
        for concept in tree.findall('.//cartapore20:'+ camelize(namespace) , ns):
            print(concept.attrib)

            # Get the fields for identificacion element
            element_id = init + offset  # identificacion
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            query_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(query_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to "+camelize(namespace)+" data to database..."):
                time.sleep(1)  # Simulate work being done

            save_query = ()
            for query_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_query = (cmex_api_controls_files_id, element_id, query_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_query)
                    cursor.commit()





        # === === === === === === === ===  remolques  === === === === === === === ===

        init = 18
        offset = 0 # the normal is zero
        step = 0
        namespace = "tipos_figura"

        print('[cyan]Go inside '+camelize(namespace)+' :[cyan]')
        for concept in tree.findall('.//cartapore20:'+ camelize(namespace) , ns):
            print(concept.attrib)

            # Get the fields for identificacion element
            element_id = init + offset  # identificacion
            offset = offset + step
            print("[red] element_id : "+str(element_id)+"[red]")
            query_element = "select id,cmex_api_tagname from sistemas.dbo.cmex_api_tags where cmex_api_section_id = ?"
            cursor.execute(query_element, (element_id,))

            elements = cursor.fetchall()

            for ids, ele in elements:
                print("[red]"+str(ids)+"[red] : [blue]"+str(ele)+"[blue]")

            for i in track(range(2), description="Saving to "+camelize(namespace)+" data to database..."):
                time.sleep(1)  # Simulate work being done

            save_query = ()
            for query_id, name in elements:
                if( concept.attrib[camelize(name)] ) :
                    saved_query = (cmex_api_controls_files_id, element_id, query_id,
                                              concept.attrib[camelize(name)], created, modified, status,)
                    cursor.execute(element_qry, saved_query)
                    cursor.commit()



        # === === === === === === === ===  TFD11  === === === === === === === ===
                print("[blue] Start TFD11 DATA  XTRACTION [blue]")
                print(cfdi_data['tfd11'])


    else:
        print("[blue] The file : "+str(filename)+" already exists  [blue]")
cursor.close()


        # if __name__ == "__main__":
        #    main();
