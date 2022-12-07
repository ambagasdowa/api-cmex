# Example : One way to search and explore this XML example is to manually add the URI to every tag or attribute in the xpath of a find() or findall():
#
#root = fromstring(xml_text)
# for actor in root.findall('{http://people.example.com}actor'):
#    name = actor.find('{http://people.example.com}name')
#    print(name.text)
#    for char in actor.findall('{http://characters.example.com}character'):
#        print(' |-->', char.text)
# A better way to search the namespaced XML example is to create a dictionary with your own prefixes and use those in the search functions:
#
# ns = {'real_person': 'http://people.example.com',
#      'role': 'http://characters.example.com'}
#
# for actor in root.findall('real_person:actor', ns):
#    name = actor.find('real_person:name', ns)
#    print(name.text)
#    for char in actor.findall('role:character', ns):
#        print(' |-->', char.text)


# Sólo uno de los nodos de cfdi:Impuestos contiene el atributo TotalImpuestosTrasladados
#
# esto bastaría para encontrar cual de ellos tiene el atributo.
#
# for node in dom.getElementsByTagName("cfdi:Impuestos"):
# print node.getAttribute("TotalImpuestosTrasladados")
#
# Aunque con lxml.etree y algo de xpath podrías hacer lo mismos.
#
#
#from xml.dom.minidom import parse, parseString
#from lxml import etree as ET
#
#dom = parse("cfdi.xml")
#
# print("-------------------------")
#
# for node in dom.getElementsByTagName("cfdi:Impuestos"):
# print(node.getAttribute("TotalImpuestosTrasladados"))
#
# Con lxml.etree
#
#d = ET.parse("cfdi.xml")
#
#ns = {"cfdi":"h t t p : / / www . sat.gob.mx / cfd / 3"}
# print("-------------------------")
# ---------------------------
#node = d.findall("//{h t t p : / / www . sat.gob.mx / cfd / 3}Impuestos/[@TotalImpuestosTrasladados]")[0]
#
# for key,val in node.items():
# print(key,val)
#
# print(node.xpath("@TotalImpuestosTrasladados")[0])
#
# ---------------------------
# print("--------------------------")
#node = d.findall("//cfdi:Impuestos/[@TotalImpuestosTrasladados]",ns)[0]
#
# for key,val in node.items():
# print(key,val)
#
# print(node.xpath("@TotalImpuestosTrasladados")[0])
#
# ---------------------------
# print("----------------------------")
#E = ET.XPathEvaluator(d,namespaces=ns)
#
# print(E("//cfdi:Impuestos/@TotalImpuestosTrasladados")[0])

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

print("[green]Connecting...[green]")

for i in track(range(2), description="Processing..."):
    time.sleep(1)  # Simulate work being done

#
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


# NOTE Better aproach

# path xml que queremos transformar
path_xml = "./F7549E3A-4EAF-48C1-9F40-C701A4D0CCB0_FAC_46_11052022_014243.xml"
transformer = CFDI40SAXHandler()  # Cfdi 4.0
# transformer = CFDI40SAXHandler().use_concepts_cfdi40()  # Cfdi 4.0
cfdi_data = transformer.transform_from_file(path_xml)

print("[blue] Overview of the CFDI [blue]")
print(cfdi_data)
print("[blue] Start CFDI XTRACTION [blue]")


def nested_dict_pairs_iterator(dict_obj):
    ''' This function accepts a nested dictionary as argument
        and iterate over all values of nested dictionaries
    '''
    # Iterate over all key-value pairs of dict argument
    for key, value in dict_obj.items():
        # Check if value is of dict type
        if isinstance(value, dict):
            # If value is dict then iterate over all its values
            print("[cyan] Layout 2 [cyan]")
            for pair in nested_dict_pairs_iterator(value):
                yield (key, *pair)
        else:
            # If value is not dict type then yield the value
            print("[blue] Layout 1 [blue]")
            yield (key, value)


# Loop through all key-value pairs of a nested dictionary
for pair in nested_dict_pairs_iterator(cfdi_data):
    print(pair)


formatter = EfiscoCorpCFDI40Formatter(cfdi_data)
if formatter.can_format():  # Verifica si puede formatear el objeto
    # Obtiene la información en formato columnar
    result_columns = formatter.dict_to_columns()
    columns = formatter.get_columns_names()  # Obtiene los headers de las columnas
#    print(result_columns)  # Contenido del xml Ej: ['3.3', 'A5', '5511', ...]
    # Nombre de las columnas Ej: ['VERSION', 'SERIE', 'FOLIO', ...]
#    print(columns)
    dict_columns = dict(zip(formatter.get_columns_names(), result_columns[0]))
    print(dict_columns)
else:
    print(formatter.get_errors())  # Not tfd11 in data.


# NOTE Search for addendas

formatter = CDACFDI40Formatter(cfdi_data)
if formatter.can_format():
    data_columns = formatter.dict_to_columns()
    print("[red] Has addenda : " + str(int(formatter.has_addenda()))+"[red]")
else:
    print(formatter.get_errors())


# Create a sample collection
#users = {'Hans': 'active', 'Éléonore': 'inactive', '景太郎': 'active'}
#print("[blue] List example [blue]")
# list(cfdi_data['tfd11'])


# https://stackoverflow.com/questions/43752962/how-to-iterate-through-a-nested-dict
#print("[blue] Collection example [blue]")
# Strategy:  Iterate over a copy
# for cfdi, values in cfdi_data.items():
#    #print("[cyan]"+cfdi+"[cyan] [red]"+values+"[red]")
#    print("[cyan]"+cfdi+"[cyan]")
#    if isinstance(values, dict):
#        print("[red]Is a instance of dict[red]")
#        for el, attr in values.items():
#            print("[red]"+el+"[red]")
#            if isinstance(attr, dict):
#                print("[blue]Is a instance of dict[blue]")
#                for elements, attributes in attr.copy().items():
#                    print("[green]"+elements+"[green]")

#    if status == 'inactive':
#        del users[user]

# Strategy:  Create a new collection
#active_users = {}
# for user, status in users.items():
#    if status == 'active':
#        active_users[user] = status


#
