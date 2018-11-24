import pyodbc
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import pdb

DEBUG = True

golden_records_key = []
golden_records_val = []

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
    item = [ii[5], ii[6], ii[8], ii[9], birthday]
    entity = build_record(item)

    if not entity:
        return None, -1

    return process.extractOne(entity, golden_records_val, scorer=fuzz.token_set_ratio)

def main(cursor):
    global golden_records_key
    global golden_records_val

    # DUMP WEBHISTO
    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Personen]([Naam],[Voorname],[RRNummer],[GebDatum], [PECLEUNIK])
                      SELECT Min(FullName), Min(FirstName), RRNUMMER, Min(DateOfBirth) , Min(PECLEUNIK)
                      FROM [Staging].[dbo].[STG_RRWebHisto_Personen] 
                      WHERE processed is null
                      GROUP BY RRNUMMER;''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Adressen]([Straat], [Postcode], [Van], [PersonenKey])
                      (   SELECT  [Street], [PostalCode], [DateAddress], [Omerus].[dbo].[STG_GR_Personen].PersonenKey
                          FROM [Staging].[dbo].[STG_RRWebHisto_Personen]
                          INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                          [Staging].[dbo].[STG_RRWebHisto_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and [Staging].[dbo].[STG_RRWebHisto_Personen].processed is null
                      );''')

    # update set processed = true
    cursor.execute("update [Staging].[dbo].[STG_RRWebHisto_Personen] set processed=1 where RRNUMMER in (select RRNummer from [Omerus].[dbo].[STG_GR_Personen]);")
    cursor.commit()

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Personen]([Naam],[Voorname],[RRNummer],[GebDatum], [PECLEUNIK])
                      (   SELECT Min(Naam), Min(voornaam), RRNUMMER, Min(GEBDATUM) , Min(PECLEUNIK)
                          FROM [Staging].[dbo].[STG_Personen] 
                          WHERE processed is null and Grade in ('A', 'B')
                          GROUP BY RRNUMMER
                      )''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Adressen]([Straat], [Postcode], [Gemeente], [PersonenKey])
                      (   SELECT  [STRAAT], [POSTKODE], [GemeenteOrig], [Omerus].[dbo].[STG_GR_Personen].PersonenKey
                          FROM [Staging].[dbo].[STG_Personen]
                          INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                          [Staging].[dbo].[STG_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and [Staging].[dbo].[STG_Personen].processed is null
                      )''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Contacten]([Telefoon], [Email], [Gemeente], [GSM], [Taalcode], [Van], [Tot], [PersonenKey]) 
                      (   SELECT  [TELEFOON], [EMAIL], [GemeenteOrig], [GSM], [TAALKODE], NULL, NULL, [Omerus].[dbo].[STG_GR_Personen].PersonenKey
                          FROM [Staging].[dbo].[STG_Personen]
                          INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                          [Staging].[dbo].[STG_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and [Staging].[dbo].[STG_Personen].processed is null
                      )''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Link](PECLEUNIK, PersonenKey, [Percent]) 
                      (   SELECT [Omerus].[dbo].[STG_GR_Personen].PECLEUNIK, [Omerus].[dbo].[STG_GR_Personen].PersonenKey, 100
                          FROM [Staging].[dbo].[STG_Personen]
                          INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                          [Staging].[dbo].[STG_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and [Staging].[dbo].[STG_Personen].processed is null
                      )''')

    cursor.execute("update [Staging].[dbo].[STG_Personen] set processed=1 where RRNUMMER in (select RRNummer from [Omerus].[dbo].[STG_GR_Personen]);")
    cursor.commit()

    # golden_records
    sql = "SELECT [PersonenKey], [Naam], [Voorname], [BTWNR], [RRNummer], [GebDatum] FROM [dbo].[STG_GR_Personen];"
    res = cursor.execute(sql).fetchall()

    golden_records = { int(ii[0]): build_record(ii[1:]) for ii in res }

    if not golden_records:
        return

    golden_records_key = list(golden_records.keys())
    golden_records_val = list(golden_records.values())

    # get records with processed = false
    while True:
        sql = "SELECT top 100 * FROM [Staging].[dbo].[STG_Personen] WHERE processed is NULL;"
        res = cursor.execute(sql).fetchall()
        if not res:
            break

        for ii in res:
            match, grade = compare(ii)

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
