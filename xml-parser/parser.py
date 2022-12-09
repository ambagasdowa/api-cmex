import pyodbc
import urllib


# importing element tree
# under the alias of ET
import xml.etree.ElementTree as ET
from rich import print

from datetime import datetime
import time
from rich.progress import track
from rich.progress import Progress


from pycfdi_transform import CFDI40SAXHandler
from pycfdi_transform.formatters.cfdi40.efisco_corp_cfdi40_formatter import EfiscoCorpCFDI40Formatter
from pycfdi_transform.formatters.cfdi40.cda_cfdi40_formatter import CDACFDI40Formatter


files_basename = '/home/ambagasdowa/github/Cmex/api-cmex/files/'
files = [
    'F7549E3A-4EAF-48C1-9F40-C701A4D0CCB0_FAC_46_11052022_014243.xml', 'D9F5AE04-485B-4110-8D0C-58E53D335167_FAC_49_11052022_070606.xml'
]

print("[green]Connecting...[green]")

for i in track(range(2), description="Processing..."):
    time.sleep(1)  # Simulate work being done

try:
    cnxn = pyodbc.connect(
        'Trusted_Connection=no;DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.8.0.235;DATABASE=sistemas;UID=zam;PWD=lis'
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
#cursor.fast_executemany = True


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


source = files_basename + files[0]
print("[blue]source file : " + source + "[blue]")


# NOTE after set the source start with the parsing :
# First get the general information

# path xml que queremos transformar
path_xml = source
transformer = CFDI40SAXHandler()  # Cfdi 4.0
# transformer = CFDI40SAXHandler().use_concepts_cfdi40()  # Cfdi 4.0
cfdi_data = transformer.transform_from_file(path_xml)

# print("[blue] Overview of the CFDI [blue]")
# print(cfdi_data)

print("[blue] Start CFDI DATA XTRACTION [blue]")
print(cfdi_data['cfdi40'])
print(cfdi_data['cfdi40'].keys())
print(cfdi_data['cfdi40'].values())


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

cmex_api_controls_files_id = 1
save_complement = (cmex_api_controls_files_id, )

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


print("[blue] NewTuple [blue]")
print(save_complement)
print(fields)
print(type(save_complement))
print("Fields are : " + str(type(fields)))

print("Count of the element : " + str(len(save_complement)))
# insert into db

insert = "insert into sistemas.dbo.cmex_api_cfdi_comprobante(cmex_api_controls_files_id, version, serie, folio, fecha,no_certificado, subtotal, descuento, total, moneda,tipo_cambio, tipo_comprobante, metodo_pago, forma_pago ,condiciones_pago,exportacion, lugar_expedicion, sello, certificado ,cmex_api_standings_id,cmex_api_parents_id, created, modified, _status) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
#print(insert)

#insert = "insert into sistemas.dbo.cmex_api_cfdi_comprobante(cmex_api_controls_files_id,sello,certificado) values(?,?,?)"
#bulk = (cmex_api_controls_files_id, save_complement[17], save_complement[18],)
# NOTE Working from hir
for i in track(range(2), description="Saving to database..."):
    time.sleep(1)  # Simulate work being done

count = cursor.execute(insert, save_complement)
#count = cursor.execute(insert, bulk)
cursor.commit()
print('Rows inserted: ' + str(count))
# print(str(count))
cursor.close()
#connection.close()


print("[blue] Start TFD11 DATA  XTRACTION [blue]")
print(cfdi_data['tfd11'])


print("[blue] Start CartaPorte20 DATA  XTRACTION [blue]")
tree = ET.parse(source)
# getting the parent tag of
# the xml document
root = tree.getroot()

# https://docs.python.org/es/3.9/library/xml.etree.elementtree.html
ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4',
      'cartapore20': 'http://www.sat.gob.mx/CartaPorte20'}


# for child in root:
#     print(child.tag, child.attrib)

#     for concept in tree.findall('.//cfdi:Concepto', ns):
#         print(concept.tag, concept.attrib)

#     print('[cyan]Impuestos N rows :[cyan]')
#     for concept in tree.findall('.//cfdi:Impuestos', ns):
#         print(concept.tag, concept.attrib)
#         print('Impuestos : Traslado y Retenciones')
#         traslado = concept.find('.//cfdi:Traslado', ns)
#         print(traslado.tag, traslado.attrib)
#         retencion = concept.find('.//cfdi:Retencion', ns)
#         print(retencion.tag, retencion.attrib)

#        for concept in tree.findall('.//cfdi:Traslado', ns):
#            print(concept.tag, concept.attrib)
#        for concept in tree.findall('.//cfdi:Retencion', ns):
#            print(concept.tag, concept.attrib)

print('[cyan]Go inside CartaPorte20 :[cyan]')
for concept in tree.findall('.//cartapore20:CartaPorte20', ns):
    print(concept.tag, concept.attrib)
for concept in tree.findall('.//cartapore20:Ubicacion', ns):
    print(concept.tag, concept.attrib)
for concept in tree.findall('.//cartapore20:Domicilio', ns):
    print(concept.tag, concept.attrib)


# print("[cyan]Iterate over concept :[cyan]")

# for concept in tree.findall('.//cfdi:Concepto', ns):
#     print(concept.tag, concept.attrib)
# #    name = actor.find('real_person:name', ns)
# #    print(name.text)
# #    for char in actor.findall('role:character', ns):
# #        print(' |-->', char.text)


# for child in root:
#    print(child.tag, child.attrib)
#    if child.tag == '':
# for concepto in root.iter('cfdi:Concepto'):
#    print(concepto.attrib)

# printing the attributes of the
# first tag from the parent
# print(root[0].attrib)

# printing the text contained within
# first subtag of the 5th tag from
# the parent
# print(root[5][0].text)

# NOTE Better aproach

# path_xml = "/home/ambagasdowa/github/Cmex/xml-parser/F7549E3A-4EAF-48C1-9F40-C701A4D0CCB0_FAC_46_11052022_014243.xml"  #path xml que queremos transformar
# transformer = CFDI40SAXHandler() # Cfdi 4.0
#cfdi_data = transformer.transform_from_file(path_xml)
#
#print("[blue] Start CFDI XTRACTION [blue]")
# print(cfdi_data)
#
