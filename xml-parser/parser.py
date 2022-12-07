import pyodbc
import urllib


# importing element tree
# under the alias of ET
import xml.etree.ElementTree as ET
from rich import print

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


cnxn = pyodbc.connect(
    'Trusted_Connection=no;DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.8.0.235;DATABASE=sistemas;UID=zam;PWD=lis'
)

print("[blue]DB Connected...[blue]")

#idPerson = 2
cursor = cnxn.cursor()
#cursor.execute('SELECT id, xml FROM prueba WHERE id=?', (idPerson,))

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

print("[blue] Overview of the CFDI [blue]")
print(cfdi_data)
print("[blue] Start CFDI DATA XTRACTION [blue]")
print(cfdi_data['cfdi40'])
print(cfdi_data['cfdi40'].keys())

print("[blue] Start TFD11 DATA  XTRACTION [blue]")
print(cfdi_data['tfd11'])

tree = ET.parse(source)

# getting the parent tag of
# the xml document
root = tree.getroot()

# printing the root (parent) tag
# of the xml document, along with
# its memory location
print(root)

# https://docs.python.org/es/3.9/library/xml.etree.elementtree.html
ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4',
      'cartapore20': 'http://www.sat.gob.mx/CartaPorte20'}

for child in root:
    print(child.tag, child.attrib)

    for concept in tree.findall('.//cfdi:Concepto', ns):
        print(concept.tag, concept.attrib)

    print('[cyan]Impuestos N rows :[cyan]')
    for concept in tree.findall('.//cfdi:Impuestos', ns):
        print(concept.tag, concept.attrib)
        print('Impuestos : Traslado y Retenciones')
        traslado = concept.find('.//cfdi:Traslado', ns)
        print(traslado.tag, traslado.attrib)
        retencion = concept.find('.//cfdi:Retencion', ns)
        print(retencion.tag, retencion.attrib)

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


print("[cyan]Iterate over concept :[cyan]")

for concept in tree.findall('.//cfdi:Concepto', ns):
    print(concept.tag, concept.attrib)
#    name = actor.find('real_person:name', ns)
#    print(name.text)
#    for char in actor.findall('role:character', ns):
#        print(' |-->', char.text)


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
