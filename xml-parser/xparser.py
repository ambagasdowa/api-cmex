#Example : One way to search and explore this XML example is to manually add the URI to every tag or attribute in the xpath of a find() or findall():
#
#root = fromstring(xml_text)
#for actor in root.findall('{http://people.example.com}actor'):
#    name = actor.find('{http://people.example.com}name')
#    print(name.text)
#    for char in actor.findall('{http://characters.example.com}character'):
#        print(' |-->', char.text)
#A better way to search the namespaced XML example is to create a dictionary with your own prefixes and use those in the search functions:
#
#ns = {'real_person': 'http://people.example.com',
#      'role': 'http://characters.example.com'}
#
#for actor in root.findall('real_person:actor', ns):
#    name = actor.find('real_person:name', ns)
#    print(name.text)
#    for char in actor.findall('role:character', ns):
#        print(' |-->', char.text)



#Sólo uno de los nodos de cfdi:Impuestos contiene el atributo TotalImpuestosTrasladados
#
#esto bastaría para encontrar cual de ellos tiene el atributo.
#
#for node in dom.getElementsByTagName("cfdi:Impuestos"):
#print node.getAttribute("TotalImpuestosTrasladados")
#
#Aunque con lxml.etree y algo de xpath podrías hacer lo mismos.
#
#
#from xml.dom.minidom import parse, parseString
#from lxml import etree as ET
#
#dom = parse("cfdi.xml")
#
#print("-------------------------")
#
#for node in dom.getElementsByTagName("cfdi:Impuestos"):
#print(node.getAttribute("TotalImpuestosTrasladados"))
#
##Con lxml.etree
#
#d = ET.parse("cfdi.xml")
#
#ns = {"cfdi":"h t t p : / / www . sat.gob.mx / cfd / 3"}
#print("-------------------------")
##---------------------------
#node = d.findall("//{h t t p : / / www . sat.gob.mx / cfd / 3}Impuestos/[@TotalImpuestosTrasladados]")[0]
#
#for key,val in node.items():
#print(key,val)
#
#print(node.xpath("@TotalImpuestosTrasladados")[0])
#
##---------------------------
#print("--------------------------")
#node = d.findall("//cfdi:Impuestos/[@TotalImpuestosTrasladados]",ns)[0]
#
#for key,val in node.items():
#print(key,val)
#
#print(node.xpath("@TotalImpuestosTrasladados")[0])
#
##---------------------------
#print("----------------------------")
#E = ET.XPathEvaluator(d,namespaces=ns)
#
#print(E("//cfdi:Impuestos/@TotalImpuestosTrasladados")[0])

import pyodbc
import urllib


# importing element tree
# under the alias of ET
import xml.etree.ElementTree as ET
from rich import print

import time
from rich.progress import track
from rich.progress import Progress


from  pycfdi_transform import CFDI40SAXHandler


print("[green]Connecting...[green]")

for i in track(range(2), description="Processing..."):
    time.sleep(1)  # Simulate work being done

#with Progress() as progress:
#
#    task1 = progress.add_task("[red]Downloading...", total=1000)
#    task2 = progress.add_task("[green]Processing...", total=1000)
#    task3 = progress.add_task("[cyan]Cooking...", total=1000)
#
#    while not progress.finished:
#        progress.update(task1, advance=0.5)
#        progress.update(task2, advance=0.3)
#        progress.update(task3, advance=0.9)
#        time.sleep(0.02)
#
cnxn = pyodbc.connect(
    'Trusted_Connection=no;DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.8.0.235;DATABASE=sistemas;UID=zam;PWD=lis'
)

print("[blue]DB Connected...[blue]")



#idPerson = 2
cursor = cnxn.cursor()
#cursor.execute('SELECT id, xml FROM prueba WHERE id=?', (idPerson,))

#working example
#cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
#for row in cursor.fetchall():
#    print(row)



# Passing the path of the
# xml document to enable the
# parsing process
# parsing as xargs

tree = ET.parse(
    '/home/ambagasdowa/github/Cmex/xml-parser/F7549E3A-4EAF-48C1-9F40-C701A4D0CCB0_FAC_46_11052022_014243.xml')

# getting the parent tag of
# the xml document
root = tree.getroot()

# printing the root (parent) tag
# of the xml document, along with
# its memory location
print(root)

#https://docs.python.org/es/3.9/library/xml.etree.elementtree.html
ns = {'cfdi': 'http://www.sat.gob.mx/cfd/4',
      'cartapore20': 'http://www.sat.gob.mx/CartaPorte20'}

for child in root:
    print(child.tag, child.attrib)

    for concept in tree.findall('.//cfdi:Concepto', ns):
        print(concept.tag, concept.attrib)

    print('[cyan]Impuestos N rows :[cyan]')
    for concept in tree.findall('.//cfdi:Impuestos', ns):
        print(concept.tag, concept.attrib)
        print ('Impuestos : Traslado y Retenciones')
        traslado = concept.find('.//cfdi:Traslado',ns)
        print(traslado.tag, traslado.attrib)
        retencion = concept.find('.//cfdi:Retencion',ns)
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




#for child in root:
#    print(child.tag, child.attrib)
#    if child.tag == '':
#for concepto in root.iter('cfdi:Concepto'):
#    print(concepto.attrib)

# printing the attributes of the
# first tag from the parent
#print(root[0].attrib)

# printing the text contained within
# first subtag of the 5th tag from
# the parent
# print(root[5][0].text)

## NOTE Better aproach

#path_xml = "/home/ambagasdowa/github/Cmex/xml-parser/F7549E3A-4EAF-48C1-9F40-C701A4D0CCB0_FAC_46_11052022_014243.xml"  #path xml que queremos transformar
#transformer = CFDI40SAXHandler() # Cfdi 4.0
#cfdi_data = transformer.transform_from_file(path_xml)
#
#print("[blue] Start CFDI XTRACTION [blue]") 
#print(cfdi_data) 
#
