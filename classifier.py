import pyodbc
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import pdb

T1 = 90
T2 = 85
LONG_CMP = False

def build_record(lst):
    return ' '.join([str(jj).lower().replace('.', '').replace(',', ' ') 
                    for jj in lst if jj])


def process_record(cursor, ii, key, grade):
    # insert into adressen
    sql = '''INSERT INTO [dbo].[STG_GR_Adressen]([Straat],[Postcode],[Gemeente],[PersonenKey]) 
             values (?, ?, ?, ?)'''
    cursor.execute(sql, (ii[13], ii[14], ii[15], key))
    # insert into contacten
    sql = '''INSERT INTO [dbo].[STG_GR_Contacten]([Telefoon],[Email],[Gemeente],[GSM],[Taalcode],[PersonenKey]) 
             values (?, ?, ?, ?, ?, ?)'''
    cursor.execute(sql, (ii[17], ii[28], ii[15], ii[29], ii[22], key))
    # insert into key
    sql = '''INSERT INTO [dbo].[STG_GR_Link]([PECLEUNIK],[PersonenKey],[Percent]) values (?, ?, ?)'''
    cursor.execute(sql, (ii[0], key, grade))
    # set processed = true
    sql = "update [Staging].[dbo].[STG_Personen] set processed=1 where PECLEUNIK={};".format(ii[0])
    cursor.execute(sql)
    cursor.commit()


def compare(ii, golden_records_val, golden_records_key):
    birthday = str(ii[11].date()) if ii[11] else ''
    item_v = [ii[5], ii[6], ii[9], birthday]
    entity_v = build_record(item_v)   # with VAT
    item = [ii[5], ii[6], birthday]
    entity = build_record(item)

    if not entity_v or not entity:
        return None, None
    if entity.count(' ') < 1 and LONG_CMP:
        return None, None

    ratio = process.extract(entity_v, golden_records_val, scorer=fuzz.partial_ratio, limit=1)
    grade = ratio[0][1]
    if(grade < T1):
        ratio = process.extract(entity, golden_records_val, scorer=fuzz.partial_ratio, limit=1)
        grade = ratio[0][1]
    return ratio, grade


def main(cursor):
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
            ratio, grade = compare(ii, golden_records_val, golden_records_key)
            if not ratio:
                continue

            if grade > T1:
                match = ratio[0][0]
                key = golden_records_key[golden_records_val.index(match)]
                print ('----------------')
                print (ii, '||', match, '||', grade, key)
            else:
                grade = 100
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

            process_record(cursor, ii, key, grade)

    # get records with processed = false
    while True:
        sql = "SELECT top 100 * FROM [Staging].[dbo].[STG_Personen] WHERE processed is NULL;"
        res = cursor.execute(sql).fetchall()
        if not res:
            break

        for ii in res:
            ratio, grade = compare(ii, golden_records_val, golden_records_key)
            if not ratio:
                continue

            if grade > T2:
                match = ratio[0][0]
                key = golden_records_key[golden_records_val.index(match)]
                process_record(ii, key)
                print ('----------------')
                print (ii, '||', match, '||', grade, key)


if __name__ == '__main__':
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=LocalHost;"
                          "Database=Omerus;"
                          "Trusted_Connection=yes;")
    cursor = cnxn.cursor()

    main(cursor)

    cursor.close()
    cnxn.close()
