import pyodbc
import re
from collections import namedtuple,OrderedDict
from functools import partial
#cnxn = pyodbc.connect('DSN=icp')
#cnxn = pyodbc.connect(r'DRIVER={MYOAU1001};TYPE=MYOB;UID=Administrator;DATABASE=C:/Users/tim/myob_simple_data_retrieval/CPCMYOB.myo;HOST_EXE_PATH=C:/Enterprise19/MYOBP.exe;')
import sqlite3
import datetime
from typing import MutableMapping,List,Tuple
import logging
logger = logging.getLogger('myob_sales_extract')

def make_myob_cnxn(dsn):
    print ("MYOB Connection started")
    try:
        cnxn = pyodbc.connect('DSN={}'.format(dsn))
    except Exception as e:
        logger.info("Exception while making myob connection with DSN={}".format(dsn))
        logger.info("The exception type is: {} and message: {}".format(type(e),e))
        raise
    logger.info("MYOB connection established")
    print ("MYOB Connection established")
    return cnxn

def read_myob_table(myob_cnxn,tablename):
    cursor = myob_cnxn.cursor()
    cursor.execute("select * from {}".format(tablename))
    rows = cursor.fetchall()
    return rows,cursor.description

def convert_row(row,table_desc:OrderedDict):
    # converts a row into SQL ready for insert
    value_list = []
    for (col_name,col_type),value in zip(table_desc.items(),row):
        if col_type in 'text':
            if not value:
                value_list.append('NULL')
            else:
                value_1 = str(value).replace("'","''")
                value_list.append("'{}'".format(value_1))
        else:
            value_list.append(str(value))

    return ','.join(value_list)

def make_and_load_sqlite_table(db:sqlite3.Connection, table_name:str, col_desc:List[Tuple], rows:MutableMapping):
    # creates a table, loads rows
    field_defs = []
    table_desc = OrderedDict()
    for field_name, field_type,_,_,_,_,_ in col_desc:
        if field_type == int:
            sqlite_type = 'integer'
        elif field_type == float:
            sqlite_type='real'
        elif field_type == str:
            sqlite_type= 'text'
        elif field_type == datetime.date:
            sqlite_type = 'text'
        table_desc[field_name] = sqlite_type

        field_def = "{field_name} {sqlite_type}".format(field_name=field_name,sqlite_type=sqlite_type)
        # print (field_def)
        field_defs.append(field_def)
    fields_string = ','.join(field_defs)

    sql_a = """DROP TABLE if EXISTS {tablename}""".format(tablename=table_name)

    sql_b = """CREATE TABLE if not EXISTS {tablename} ( {fields})""".format(tablename=table_name,fields=fields_string)

    cur=db.cursor()
    result = cur.execute(sql_a)
    result = cur.execute(sql_b)
    #print (sql_b)

   # print ('table made')
    for r in rows:
        value_string = convert_row(r,table_desc)
        sql_ins = "Insert into {tablename} ({columns}) VALUES ( {values} )".format(
            tablename=table_name,columns=','.join(table_desc), values=value_string)
        try:
            r = cur.execute(sql_ins)
        except sqlite3.OperationalError:
            print (sql_ins)
            raise
    db.commit()
    return

def create_item_unit_table(db):
    # this table is used as a one off to deduce UOM from product descriptions
    # the MYOB field is limited to 5 chars
    sql_a = """DROP TABLE if EXISTS {tablename}""".format(tablename='item_unit')
    cur = db.cursor()
    result = cur.execute(sql_a)
    sql_b = """CREATE TABLE if not exists item_unit (ItemNumber text PRIMARY KEY, ItemName TEXT, UOM_qty REAL,
      MYOB_UOM TEXT) """
    result = cur.execute(sql_b)

def create_uom_conversion_table(db):
    # this table is used to convert the sold units into metric and imperial units

    # the MYOB field is limited to 5 chars
    sql_a = """DROP TABLE if EXISTS {tablename}""".format(tablename='uom_conversion')
    cur = db.cursor()
    result = cur.execute(sql_a)
    sql_b = """CREATE TABLE if not exists uom_conversion (ItemID integer PRIMARY KEY, metric_uom TEXT,
imperial_uom TEXT, native_uom TEXT, qty_to_metric REAL, qty_to_imperial REAL) """
    result = cur.execute(sql_b)

def create_last_changed_table(db):
    cur = db.cursor()
    sql_b = """CREATE TABLE if not exists master_last_changed (ID integer PRIMARY KEY, table_name TEXT,
    record_id integer, change_control TEXT) """
    result = cur.execute(sql_b)
    sql_c = """CREATE UNIQUE INDEX if not exists change on master_last_changed (table_name, record_id) """
    result = cur.execute(sql_c)

def create_sqlite_connection(db_file)->sqlite3.Connection:
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
    return None

def get_elem(row, source_fields, list_index):
    # source_fields is a list of attributes in row. Return exactly the nth attribute
    return getattr(row, source_fields[list_index])

cust_map = OrderedDict([
    ('Customer',partial(get_elem,**{'source_fields':['Name'],'list_index':0})),
    ('Customer_ID',partial(get_elem,**{'source_fields':['CardRecordID'],'list_index':0})),
])


def load_tables(sqlite_db,myob_cnxn,table_names=None):
    if not table_names:
        table_names = ('Customers','Employees','Items','Address','CustomLists','Sales','SaleLinesAll',
                       'InvoiceType','Status','ServiceSaleLinesAll','ItemSaleLinesAll','Currency','Accounts',
        'Items','ItemLocations','Locations','AccountActivities','DataFileInformation','JournalRecords',
                       # 'ItemMovement','ItemOpeningBalance','JournalSets','InventoryAdjustments',
                       # 'InventoryTransfers'
                       )
    for table_name in table_names:
        rows,table_desc = read_myob_table(myob_cnxn,table_name)
        make_and_load_sqlite_table(sqlite_db,table_name=table_name,col_desc=table_desc,rows=rows)

def load_metadata(sqlite_db):
    pass

def populate_uom_conversion_table(sqlite_db):
    # read MYOB's UOM and create a table that is used in the SQL to create the sales extract
    # UOMs are digits and a unit code. The unit code is usally one letter
    #
    def stringifyme(v):
        if v:
            return "'%s'"%v
        else:
            return 'Null' \
                   ''
    def make_sql(itemID,  metric_uom, imperial_uom,
            native_uom,qty_to_metric, qty_to_imperial):
        return  """INSERT OR REPLACE INTO uom_conversion (itemID,metric_uom,imperial_uom,native_uom,
                               qty_to_metric,qty_to_imperial)
                               VALUES ( {itemID},{metric_uom},{imperial_uom},{native_uom}, {qty_to_metric},{qty_to_imperial})
                               """.format(itemID=item_id,
                                          metric_uom=stringifyme(metric_uom), imperial_uom=stringifyme(imperial_uom),
                                          native_uom=stringifyme(native_uom),
                                          qty_to_metric=qty_to_metric,
                                          qty_to_imperial=qty_to_imperial)

    create_uom_conversion_table(sqlite_db)
    cursor = sqlite_db.cursor()
    result = cursor.execute("select ItemID,ItemName,ItemNumber,SellUnitMeasure from Items")

    uom_re = re.compile('([\d\.]*)\s?(\w+)$', flags=re.IGNORECASE)
    for row in list(result):
        item_id = row[0]
        item_name = row[1]
        sku = row[2]
        sell_uom = row[3]
        if not sell_uom:
            sql_upsert = """INSERT OR REPLACE INTO uom_conversion (itemID,metric_uom,imperial_uom,native_uom,
                                                qty_to_metric,qty_to_imperial)
                                                VALUES ( {itemID},Null,Null,'{native_uom}', 0,0)
                                                """.format(itemID=item_id, native_uom=None)

        elif sell_uom.upper() == 'EACH':
            native_unit = imperial_unit = metric_unit = 'EACH'
            qty_to_metric = qty_to_imperial = 1
            sql_upsert = make_sql(itemID=item_id,
                                          metric_uom=metric_unit, imperial_uom=imperial_unit,
                                          native_uom=native_unit,
                                          qty_to_metric=qty_to_metric,
                                          qty_to_imperial=qty_to_imperial)


        else:
            re_result = uom_re.search(sell_uom)
            if re_result:
                unit = re_result.group(2).upper()
                try:
                    qty = float(re_result.group(1).upper())
                except ValueError:
                    qty = 1
                if unit == 'L':
                    metric_uom = "LT"
                    imperial_uom = "US Gal"
                    native_unit = unit
                    qty_to_metric = qty
                    qty_to_imperial = qty / 3.785411784
                elif unit == 'ML':
                    metric_uom = "LT"
                    imperial_uom = "US Gal"
                    native_unit = unit
                    qty_to_metric = 0.001 * qty
                    qty_to_imperial = qty / 1000 / 3.785411784
                elif unit in ('K','KG'):
                    metric_uom = "KG"
                    imperial_uom = "LB"
                    native_unit = unit
                    qty_to_metric = qty
                    qty_to_imperial = qty * 2.20462
                elif unit in ('G'): #gallon
                    metric_uom = "LT"
                    imperial_uom = "US Gal"
                    native_unit = unit
                    qty_to_metric = qty * 3.785411784
                    qty_to_imperial = qty
                elif unit in ('M2'):  # square metres
                    metric_uom = "M2"
                    imperial_uom = "FT2"
                    native_unit = unit
                    qty_to_metric = qty
                    qty_to_imperial = qty * 10.7639
                elif unit in ('OZ'):  # fluid ounce
                    metric_uom = "LT"
                    imperial_uom = "FLOZ"
                    native_unit = unit
                    qty_to_metric = qty * 0.0295735
                    qty_to_imperial = qty
                elif unit in ('Q'):  # quart
                    metric_uom = "LT"
                    imperial_uom = "QT"
                    native_unit = unit
                    qty_to_metric = qty * 0.946353
                    qty_to_imperial = qty
                elif unit in ('LB'): #pound
                    metric_uom = 'KG'
                    imperial_uom = 'LB'
                    native_unit = unit
                    qty_to_metric = qty * 0.453592
                    qty_to_imperial = qty

                else:
                    metric_uom = None
                    imperial_uom = None
                    native_unit = None
                    qty_to_metric = None
                    qty_to_imperial = None


                sql_upsert = make_sql(itemID=item_id,
                                              metric_uom=metric_uom, imperial_uom=imperial_uom,
                                              native_uom=native_unit,
                                              qty_to_metric=qty_to_metric,
                                              qty_to_imperial=qty_to_imperial)

            else:  # a unit we don't know how to handle
                native_unit = imperial_unit = metric_unit = None
                qty_to_metric = qty_to_imperial = 0
                sql_upsert = """INSERT OR REPLACE INTO uom_conversion (itemID,metric_uom,imperial_uom,native_uom,
                                    qty_to_metric,qty_to_imperial)
                                    VALUES ( {itemID},Null,Null,'{native_uom}', 0,0)
                                    """.format(itemID=item_id,native_uom=native_unit)
        try:
            sql_result = cursor.execute(sql_upsert)
        except Exception as e:
            pass
    sqlite_db.commit()
    return True


def create_myob_uoms(sqlite_db)->MutableMapping[str,Tuple[str,int]]:
    # a one off
    create_item_unit_table(sqlite_db)
    cursor = sqlite_db.cursor()

    result = cursor.execute("select ItemID,ItemName,ItemNumber from Items")
    tests = {}
    tests['L'] = re.compile('[\s-]([\d\.]*)\s*l(t)?\s*$',flags=re.IGNORECASE) #a space or a hypen, numbers or space optional, and then lt
    tests['G'] = re.compile('[\s-]([\d\.]*)\s*gal(lon)?\s*$', flags=re.IGNORECASE)
    tests['ML'] = re.compile('[\s-]([\d\.]*)\s*ml\s*$', flags=re.IGNORECASE)
    tests['K'] = re.compile('[\s-]([\d\.]*)\s*kg\s*$', flags=re.IGNORECASE)  # a space or a hypen, numbers or space optional, and then lt
    UOM_per_item = {} #type: MutableMapping[str,Tuple[str,int]]
    for row in result.fetchall():
        item_id = row[0]
        item_name = row[1]
        sku = row[2]
        found_uom = None
        found_factor = 0
        for UOM_name,test_re in tests.items():
            re_result = test_re.search(item_name)
            if re_result:
                uom_qty = re_result.group(1) or 1
                UOM_per_item[sku] = (UOM_name,re_result.group(1))
                sql_upsert = """INSERT OR REPLACE INTO item_unit (itemNumber,MYOB_UOM,uom_qty,itemName)
VALUES ( '{itemNumber}','{MYOB_UOM}',{uom_qty},'{itemName}')""".format(itemNumber=sku,
                    MYOB_UOM=UOM_name,
                    uom_qty=uom_qty,
                    itemName=item_name.replace("'","''"))
               # print (sql_upsert)
                result =  cursor.execute(sql_upsert)
                break
        else:
            UOM_per_item[sku] = (None,None)
            sql_upsert = """INSERT OR REPLACE INTO item_unit (itemNumber,MYOB_UOM,uom_qty,itemName)
            VALUES ( '{itemNumber}','{MYOB_UOM}',Null,'{itemName}')""".format(itemNumber=sku,
                                                                             MYOB_UOM='Each',
                                                                             itemName=item_name.replace("'", "''"))
            result = cursor.execute(sql_upsert)

    sqlite_db.commit()
    return UOM_per_item

def prepare_data(myob_cnxn,db_path='myob_extract.sqlite',tables=None):
    sqlite_db = create_sqlite_connection(db_path)
    #create_sales_metadata_table(sqlite_db)
    load_tables(myob_cnxn=myob_cnxn,sqlite_db=sqlite_db,table_names=tables)
    #UOM_per_item = create_myob_uoms(sqlite_db)
    populate_uom_conversion_table(sqlite_db)
    create_last_changed_table(sqlite_db)
   # print(UOM_per_item)

def make_sales_extract():
    sqlite_db = create_sqlite_connection('myob_extract.sqlite')
    cursor = sqlite_db.cursor()

if __name__ == '__main__':

    prepare_data(make_myob_cnxn(dsn='CPC_Myob'),'myob_extract.sqlite')

"""
select c.name as 'Customer',c.CustomerID as 'Customer ID',c.CardRecordID as 'Customer MYOB ID',
  c.name as 'ShipTo',c.CustomerID as 'ShipTo ID', null as 'Customer SalesPerson',null as 'ShipTo SalesPerson',
  shipto.Street,shipto.City,shipto.State,shipto.Country,
  billto.street,billto.city,billto.state,billto.country,
  cl1.CustomListText as 'CustomList1',
  cl2.CustomListText as 'CustomList2',
  cl3.CustomListText as 'CustomList3'

from customers as c
  left outer join address as billto on billto.location = 1 and billto.cardrecordid = c.customerid
  left outer join address as shipto on shipto.location = 2 and shipto.cardrecordid = c.customerid
  left outer join customlists as cl1 on cl1.CustomListID = c.CustomList1ID
  left outer join customlists as cl2 on cl2.CustomListID = c.CustomList2ID
  left outer join customlists as cl3 on cl3.CustomListID = c.CustomList3ID
"""