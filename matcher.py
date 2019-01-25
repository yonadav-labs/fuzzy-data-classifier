import pyodbc
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import uuid
import pdb

DEBUG = True

T1 = 90

golden_records_key = []
golden_records_val = []

def build_record(lst):
    return ' '.join([str(jj).lower().replace('.', '').replace(',', ' ') 
                    for jj in lst if jj])


def process_record(cursor, ii, key, grade, match):
    # insert into corresponding tables
    sql = '''INSERT INTO [dbo].[STG_GR_Adressen]([Straat],[Postcode],[Gemeente],[PersonenKey]) 
             values (?, ?, ?, ?)'''
    cursor.execute(sql, (ii.STRAAT, ii.POSTKODE, ii.GemeenteOrig, key))

    sql = '''INSERT INTO [dbo].[STG_GR_Contacten]([Telefoon],[Email],[Gemeente],[GSM],[Taalcode],[PersonenKey]) 
             values (?, ?, ?, ?, ?, ?)'''
    cursor.execute(sql, (ii.TELEFOON, ii.EMAIL, ii.GemeenteOrig, ii.GSM, ii.TAALKODE, key))

    sql = '''INSERT INTO [dbo].[STG_GR_Link]([PECLEUNIK],[PersonenKey],[Percent]) values (?, ?, ?)'''
    cursor.execute(sql, (ii.PECLEUNIK, key, grade))
    # set processed = true
    sql = "update [Staging].[dbo].[STG_Personen] set processed=1 where PECLEUNIK={};".format(ii.PECLEUNIK)
    cursor.execute(sql)
    cursor.commit()

    if match and DEBUG:
        print ('----------------')
        print (ii, '||', match, '||', grade, key)


def compare(ii):
    birthday = str(ii.GEBDATUM.date()) if ii.GEBDATUM else ''
    item = [ii.Naam, ii.voornaam, ii.RRNUMMER, ii.BTWNR, birthday]
    entity = build_record(item)

    if not entity:
        return None, -1

    return process.extractOne(entity, golden_records_val, scorer=fuzz.token_set_ratio)


def main(cursor):
    global golden_records_key
    global golden_records_val

    pdb.set_trace()
    # DUMP WEBHISTO
    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Personen]([Naam], [Voornaam], [Achternaam], [FirstNameAll], [RRNummer], [GebDatum])
                      SELECT Min(FullName), Min(FirstName), Min(LastName), Min(FirstNameAll), RRNUMMER, Min(DateOfBirth)
                      FROM [Staging].[dbo].[STG_RRWebHisto_Personen] 
                      WHERE processed is null and RRNUMMER is not null and RRNUMMER != ''
                      GROUP BY RRNUMMER;''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Adressen]([Straat], [Postcode], [Van], [PersonenKey])
                      SELECT  [Street], [PostalCode], [DateAddress], [Omerus].[dbo].[STG_GR_Personen].PersonenKey
                      FROM [Staging].[dbo].[STG_RRWebHisto_Personen]
                      INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                      [Staging].[dbo].[STG_RRWebHisto_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and 
                      [Staging].[dbo].[STG_RRWebHisto_Personen].processed is null and 
                      [Staging].[dbo].[STG_RRWebHisto_Personen].RRNUMMER is not null and 
                      [Staging].[dbo].[STG_RRWebHisto_Personen].RRNUMMER != '';''')

    # update set processed = true
    cursor.execute("update [Staging].[dbo].[STG_RRWebHisto_Personen] set processed=1 where RRNUMMER in (select RRNummer from [Omerus].[dbo].[STG_GR_Personen]) and RRNUMMER is not null and RRNUMMER != '';")
    cursor.commit()

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Personen]([Naam], [Voornaam], [Achternaam], [FirstNameAll], [RRNummer], [GebDatum])
                      SELECT Min(Naam), Min(voornaam), Min(Achternaam), null, RRNUMMER, Min(GEBDATUM)
                      FROM [Staging].[dbo].[STG_Personen] 
                      WHERE processed is null and Grade in ('A', 'B') and RRNUMMER is not null and RRNUMMER != ''
                      GROUP BY RRNUMMER;''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Adressen]([Straat], [Postcode], [Gemeente], [PersonenKey])
                      SELECT  [STRAAT], [POSTKODE], [GemeenteOrig], [Omerus].[dbo].[STG_GR_Personen].PersonenKey
                      FROM [Staging].[dbo].[STG_Personen]
                      INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                      [Staging].[dbo].[STG_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and 
                      [Staging].[dbo].[STG_Personen].processed is null and
                      [Staging].[dbo].[STG_Personen].RRNUMMER is not null and 
                      [Staging].[dbo].[STG_Personen].RRNUMMER != '';''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Contacten]([Telefoon], [Email], [Gemeente], [GSM], [Taalcode], [Van], [Tot], [PersonenKey]) 
                      SELECT  [TELEFOON], [EMAIL], [GemeenteOrig], [GSM], [TAALKODE], NULL, NULL, [Omerus].[dbo].[STG_GR_Personen].PersonenKey
                      FROM [Staging].[dbo].[STG_Personen]
                      INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                      [Staging].[dbo].[STG_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and 
                      [Staging].[dbo].[STG_Personen].processed is null and
                      [Staging].[dbo].[STG_Personen].RRNUMMER is not null and 
                      [Staging].[dbo].[STG_Personen].RRNUMMER != '';''')

    cursor.execute('''INSERT INTO [Omerus].[dbo].[STG_GR_Link](PECLEUNIK, PersonenKey, [Percent]) 
                      SELECT [Staging].[dbo].[STG_Personen].PECLEUNIK, [Omerus].[dbo].[STG_GR_Personen].PersonenKey, 100
                      FROM [Staging].[dbo].[STG_Personen]
                      INNER JOIN [Omerus].[dbo].[STG_GR_Personen] ON
                      [Staging].[dbo].[STG_Personen].RRNUMMER = [Omerus].[dbo].[STG_GR_Personen].RRNummer and 
                      [Staging].[dbo].[STG_Personen].processed is null and
                      [Staging].[dbo].[STG_Personen].RRNUMMER is not null and 
                      [Staging].[dbo].[STG_Personen].RRNUMMER != '';''')

    cursor.execute("update [Staging].[dbo].[STG_Personen] set processed=1 where RRNUMMER in (select RRNummer from [Omerus].[dbo].[STG_GR_Personen]) and RRNUMMER is not null and RRNUMMER != '';")
    cursor.commit()

    # golden_records
    sql = "SELECT [PersonenKey], [Naam], [Voornaam], [BTWNR], [RRNummer], [GebDatum] FROM [dbo].[STG_GR_Personen];"
    res = cursor.execute(sql).fetchall()

    golden_records = { int(ii.PersonenKey): build_record(ii[1:]) for ii in res }

    if not golden_records:
        return

    golden_records_key = list(golden_records.keys())
    golden_records_val = list(golden_records.values())

    # get records with processed = false
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
                # insert into STG_GR_Personen
                sql = '''INSERT INTO [dbo].[STG_GR_Personen]([Naam], [Voornaam], [Achternaam], [FirstNameAll], [RRNummer], [BTWnr], [Geslacht], [GebDatum], [GebPlaats])
                         values(?,?,?,?,?,?,?,?,?,?)'''
                cursor.execute(sql, (ii.Naam, ii.voornaam, str(uuid.uuid4()), ii.Achternaam, None, ii.BTWNR, ii.GESLACHT, ii.GEBDATUM, ii.GEBPLAATS))
                cursor.commit()
                # update golden_records
                sql = "SELECT top 1 [PersonenKey], [Naam], [Voornaam], [BTWNR], [GebDatum] FROM [dbo].[STG_GR_Personen] order by PersonenKey desc;"
                cursor.execute(sql)
                new = cursor.fetchone()
                key = int(new[0])
                print ("Adding a new golden record ({}) ...".format(key))
                golden_records[key] = build_record(new[1:])
                golden_records_key = list(golden_records.keys())
                golden_records_val = list(golden_records.values())
                process_record(cursor, ii, key, 100, None)

    # get records with processed = false
    while True:
        sql = "SELECT top 100 * FROM [Staging].[dbo].[STG_Personen] WHERE processed is NULL;"
        res = cursor.execute(sql).fetchall()
        if not res:
            break

        for ii in res:
            match, grade = compare(ii)

            if match:
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
