import pyodbc
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import pdb

T1 = 90
T2 = 85
LONG_CMP = False
DEBUG = True

golden_records_key = []
golden_records_val = []
golden_records_key_v = []
golden_records_val_v = []

def build_record(lst):
    return ' '.join([str(jj).lower().replace('.', '').replace(',', ' ') 
                    for jj in lst if jj])


def process_record(cursor, ii, key, grade, match):
    # insert into corresponding tables
    sql = '''INSERT INTO [dbo].[STG_GR_Adressen]([Straat],[Postcode],[Gemeente],[PersonenKey]) 
             values (?, ?, ?, ?)'''
    cursor.execute(sql, (ii[13], ii[14], ii[15], key))

    sql = '''INSERT INTO [dbo].[STG_GR_Contacten]([Telefoon],[Email],[Gemeente],[GSM],[Taalcode],[PersonenKey]) 
             values (?, ?, ?, ?, ?, ?)'''
    cursor.execute(sql, (ii[17], ii[28], ii[15], ii[29], ii[22], key))

    sql = '''INSERT INTO [dbo].[STG_GR_Link]([PECLEUNIK],[PersonenKey],[Percent]) values (?, ?, ?)'''
    cursor.execute(sql, (ii[0], key, grade))
    # set processed = true
    sql = "update [Staging].[dbo].[STG_Personen] set processed=1 where PECLEUNIK={};".format(ii[0])
    cursor.execute(sql)
    cursor.commit()

    if match and DEBUG:
        print ('----------------')
        print (ii, '||', match, '||', grade, key)


def compare(ii):
    birthday = str(ii[11].date()) if ii[11] else ''
    item_v = [ii[5], ii[6], ii[9], birthday]
    entity_v = build_record(item_v)   # with VAT
    item = [ii[5], ii[6], birthday]
    entity = build_record(item)

    if not entity_v or not entity:
        return None, -1
    if entity.count(' ') < 1 and LONG_CMP:
        return None, -1

    return process.extractOne(entity_v, golden_records_val, scorer=fuzz.token_set_ratio)


def main(cursor):
    global golden_records_key
    global golden_records_val
    global golden_records_key_v
    global golden_records_val_v
    # DUMP WEBHISTO
    cursor.execute('''INSERT INTO [dbo].[STG_GR_Personen]([Naam],[Voorname],[RRNummer],[GebDatum])
                      SELECT FullName, FirstName, Min(RRNUMMER), DateOfBirth 
                      FROM [Staging].[dbo].[STG_RRWebHisto_Personen] 
                      WHERE processed is null
                      GROUP BY FullName, FirstName, DateOfBirth;''')

    # update set processed = true
    cursor.execute("update [Staging].[dbo].[STG_RRWebHisto_Personen] set processed = 1;")
    cursor.commit()

    # golden_records
    sql = "SELECT [PersonenKey], [Naam], [Voorname], [BTWNR], [GebDatum] FROM [dbo].[STG_GR_Personen];"
    res = cursor.execute(sql).fetchall()

    golden_records = { int(ii[0]): build_record(ii[1:]) for ii in res }

    if not golden_records:
        return

    golden_records_key = list(golden_records.keys())
    golden_records_val = list(golden_records.values())

    # select records processed = false and grade in ('A', 'B')
    while True:
        sql = "SELECT top 100 * FROM [Staging].[dbo].[STG_Personen] WHERE processed is NULL and grade in ('A', 'B');"
        res = cursor.execute(sql).fetchall()
        if not res:
            break

        for ii in res:
            match, grade = compare(ii)

            if grade > T1:
                key = golden_records_key[golden_records_val.index(match)]
                process_record(cursor, ii, key, grade, match)
            elif grade > 0:
                print ("Adding a new golden record ...")
                # insert into STG_GR_Personen
                sql = '''INSERT INTO [dbo].[STG_GR_Personen]([Naam],[Voorname],[RRNummer],[BTWnr],[Geslacht],[GebDatum],[GebPlaats],[PECLEUNIK])
                         values(?,?,?,?,?,?,?,?)'''
                cursor.execute(sql, (ii[5], ii[6], ii[8], ii[9], ii[30], ii[11], ii[23], ii[0]))
                cursor.commit()
                # update golden_records
                sql = "SELECT [PersonenKey], [Naam], [Voorname], [BTWNR], [GebDatum] FROM [dbo].[STG_GR_Personen] where PersonenKey not in {};".format(tuple(golden_records_key))
                cursor.execute(sql)
                new = cursor.fetchone()
                key = int(new[0])
                golden_records[key] = build_record(new[1:])
                golden_records_key = list(golden_records.keys())
                golden_records_val = list(golden_records.values())
                process_record(cursor, ii, key, 101, None)

    # get records with processed = false
    while True:
        sql = "SELECT top 100 * FROM [Staging].[dbo].[STG_Personen] WHERE processed is NULL;"
        res = cursor.execute(sql).fetchall()
        if not res:
            break

        for ii in res:
            match, grade = compare(ii)

            if grade > T2:
                key = golden_records_key[golden_records_val.index(match)]
                process_record(cursor, ii, key, grade, match)


if __name__ == '__main__':
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=LocalHost;"
                          "Database=Omerus;"
                          "Trusted_Connection=yes;")
    cursor = cnxn.cursor()

    main(cursor)

    cursor.close()
    cnxn.close()
