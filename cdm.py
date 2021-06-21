import sys
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import re
import phonenumbers
from io import StringIO
from datetime import datetime,timedelta
from dateutil.parser import parse

try:
    connection = psycopg2.connect(
            user = 'R',
            host = '127.0.0.1',
            port = '5432',
            database = 'R',
            cursor_factory=psycopg2.extras.DictCursor
            )
    cursor = connection.cursor()

    snap_date = sys.argv[1]

#########################################################
#                 STANDARDIZE DATA                      #
#########################################################
    cursor.execute('SELECT * FROM preprocess WHERE snap_date=%(snap_date)s;',{'snap_date':snap_date})
    data = cursor.fetchall()
    cursor.execute('DELETE FROM standardize WHERE snap_date=%(snap_date)s;',{'snap_date':snap_date})

    for row in data:

        #std birthday
        if row['birthday'] is not None:
            a = parse(row['birthday'])#if need, use datepaser lib for fancy funtions
            row['birthday'] = a.strftime('%Y/%m/%d')

        #std mobile
        if row['mobile'] is not None:
            a = re.sub('\D','',row['mobile'])
            a = phonenumbers.parse(a,'TW')
            if phonenumbers.is_valid_number(a):
                row['mobile'] = phonenumbers.format_number(a, phonenumbers.PhoneNumberFormat.E164)
            else:
                row['mobile'] = None

        #std phone
        if row['phone'] is not None:
            a = phonenumbers.parse(row['phone'],'TW')
            if phonenumbers.is_valid_number(a):
                row['phone'] = phonenumbers.format_number(a, phonenumbers.PhoneNumberFormat.E164)
                if a.extension is not None:
                    row['phone'] = row['phone']+' Ext.'+a.extension
            else:
                row['phone'] = None

        #std ID number
        if row['id_type'] == 'PID':
            m = re.match('^[A-Za-z]\d{9}\W*',row['id_number'])
            if m is not None:
                a = m.group(0)[0].upper() + m.group(0)[1:10]
        elif row['id_type'] == 'TID':
            m = re.match('^\d{8}$',row['id_number'])
            if m is not None:
                a = m.group(0)
        else:
            m = None
            a = None

        if m is None:
            row['id_number'] = None
        else:
            row['id_number'] = a

        #std last_update_date
        if row['last_update_date'] is not None:
            row['last_update_date'] = parse(row['last_update_date'])

        #create time
        row['create_time'] = datetime.now()


    #insert into standardize table
    if data:
        insert_query = 'INSERT INTO standardize (' +','.join(list(data[0].keys()))+') VALUES %s;'
        psycopg2.extras.execute_values(cursor, insert_query, data)
        connection.commit()


#########################################################
#                   MATCH DATA                          #
#########################################################

    prev_date = datetime.strftime(datetime.strptime(snap_date,'%Y%m%d') - timedelta(1),'%Y%m%d')
    cursor.execute('DELETE FROM matching WHERE snap_date=%(snap_date)s;',{'snap_date':snap_date})

    new_data = pd.read_sql_query('SELECT * FROM standardize WHERE snap_date=%(snap_date)s;',connection,params={'snap_date':snap_date})
    new_data['group_id']=np.nan
    master_data = pd.read_sql_query('SELECT * FROM master_bk WHERE snap_date=%(prev_date)s;',connection,params={'prev_date':prev_date})
    master_data['snap_date']=snap_date
    data = pd.concat([master_data,new_data],ignore_index=True)

    cursor.execute('SELECT * FROM rule_match_attribute ORDER BY seq ASC;')
    attribute_rules = cursor.fetchall()

    attribute_scores = pd.read_sql_query('SELECT * FROM rule_match_score;',connection)
    attribute_scores.set_index('col_nm',inplace = True)

    cursor.execute('SELECT value1 FROM sys_parameter WHERE pname=\'confident_score\'')
    confidence_score = int(cursor.fetchone()[0])


    ####### match by attribute rules #######
    group_counter = 1
    for row in attribute_rules:
        temp = [x.strip() for x in row['match_attribute'].split(',')]
        grouped = data[data.group_id.isnull()].groupby(temp)#pick data with null group_id
        for key, group in grouped:
            if len(group) > 1:
                for lst in list(group.index):
                    data.loc[lst,'group_id'] = group_counter
                group_counter += 1

    ####### match by scores #######
    score_sum = 0
    match_counter = 0
    # match with already-matched data first
    for ind in data[data.group_id.isnull()].index:
        for ind_grouped in data[data.group_id.notnull()].index:
            for col in data.columns:
                if data.loc[ind,col] == data.loc[ind_grouped,col] and col in attribute_scores.index and data.loc[ind,col] is not None:
                    score_sum += int(attribute_scores.loc[col,'score'])
            if score_sum >= confidence_score and data.loc[ind,'group_id'] != data.loc[ind_grouped,'group_id']:
                match_counter += 1
                if match_counter == 1:
                    data.loc[ind,'group_id'] = data.loc[ind_grouped,'group_id']
                else:
                    data.loc[ind,'group_id'] = 'Multi'
            score_sum = 0
        match_counter = 0

    # then recursively cross match all ungrouped data
    ### recursive function in this block ###
    def recurse_list(ind, ind_list):
        rest_list = filter(lambda x : x!=ind, ind_list)
        for ind_rest in rest_list:
            score_sum = 0
            for col in data.columns:
                if data.loc[ind,col] == data.loc[ind_rest,col] and col in attribute_scores.index and data.loc[ind,col] is not None:
                    score_sum += int(attribute_scores.loc[col,'score'])
            if score_sum >= confidence_score:
                data.loc[ind_rest,'group_id'] = data.loc[ind,'group_id']
                recurse_list(ind_rest, rest_list)
        return
    ### end of function ###

    ind_list = data[data.group_id.isnull()].index
    while len(ind_list) != 0:
        data.loc[ind_list[0],'group_id'] = group_counter
        recurse_list(ind_list[0],filter(lambda x : x!=ind_list[0], ind_list))
        group_counter += 1
        ind_list = data[data.group_id.isnull()].index

    #put in create time in the end
    data['create_time'] = datetime.now()
    data['group_id']=data['group_id'].astype(int)

    ## write into mathing table
    sio = StringIO()
    sio.write(data.to_csv(index=None, header=None))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream

    # Copy the string buffer to the database, as if it were an actual file
    cursor.copy_from(sio, 'matching', columns=data.columns, null='', sep=',')
    connection.commit()

#########################################################
#                   MERGE DATA                          #
#########################################################

    def generate_UCID(cur_ucid):
        if cur_ucid == 0:
            cursor.execute('SELECT value1 FROM sys_parameter WHERE pname=\'ucid\'')
            old_ucid = int(cursor.fetchone()[0])
            return str(old_ucid+1).rjust(9,'0')
        else:
            return str(int(cur_ucid)+1).rjust(9,'0')

    cur_ucid = 0
    cursor.execute('DELETE FROM master;')
    cursor.execute('DELETE FROM master_src;')
    data = pd.read_sql_query('SELECT * FROM matching WHERE snap_date=%(snap_date)s;',connection,params={'snap_date':snap_date})
    master_table = pd.read_sql_query('SELECT * FROM master;', connection)
    master_src = pd.read_sql_query('SELECT * FROM master_src;',connection)
    master_src_old = pd.read_sql_query('SELECT * FROM master_src_bk WHERE snap_date=%(prev_date)s;',connection,params={'prev_date':prev_date})
    master_src_old.set_index('ucid',inplace = True)

    grouped = data.groupby('group_id')
    source_scores = pd.read_sql_query('SELECT * FROM rule_merge_score;',connection)
    source_scores.set_index(['col_nm','source_system'],inplace = True)

    index_counter = 0
    for key, group in grouped:
        for col in source_scores.index.get_level_values(0).unique():#loop through all col w/ scores
            tmp_score = 0
            tmp_index = -1
            for lst in list(group[group[col].notnull()].index):#loop through only the not empty rows
                if tmp_index == -1:
                    tmp_index = lst #assign the first row at the start
                    tmp_ucid = data.loc[lst,'ucid']
                if tmp_ucid is not None:
                    tmp_src = master_src_old.loc[tmp_ucid,col]
                else:
                    tmp_src = data.loc[lst,'source_system']
                if source_scores.index.isin([(col,tmp_src)]).any():#check the specific data has score
                    #if find a higher score scource
                    if source_scores.loc[(col,tmp_src),'score'] > tmp_score:
                        tmp_score = source_scores.loc[(col,tmp_src),'score']
                        tmp_index = lst
                        tmp_ucid = data.loc[lst,'ucid']
                    #if the same score, choose the one w/o UCID
                    elif source_scores.loc[(col,tmp_src),'score'] == tmp_score and tmp_ucid is not None and data.loc[lst,'ucid'] is None:
                        tmp_index = lst
                        tmp_ucid = None
            if tmp_index != -1:
                master_table.loc[index_counter,col] = data.loc[tmp_index,col]
                master_src.loc[index_counter,col] = tmp_src
        #fill in UCID
        tmp_ucid = 0
        for lst in list(group[group['ucid'].notnull()].index):
            tmp_ucid = data.loc[lst,'ucid']
        if tmp_ucid != 0:
            master_table.loc[index_counter,'ucid'] = tmp_ucid
            master_src.loc[index_counter,'ucid'] = tmp_ucid
        else:
            master_table.loc[index_counter,'ucid'] = generate_UCID(cur_ucid)
            cur_ucid = master_table.loc[index_counter,'ucid']
            master_src.loc[index_counter,'ucid'] = master_table.loc[index_counter,'ucid']

        index_counter += 1

    #fill in rest of the tables
    master_table['create_time'] = datetime.now()
    master_table['createby'] = 'system'
    master_src['create_time'] = datetime.now()
    master_src['createby'] = 'system'

    ## write into master table
    sio = StringIO()
    sio.write(master_table.to_csv(index=None, header=None))
    sio.seek(0)
    cursor.copy_from(sio, 'master', columns=master_table.columns, null='', sep=',')

    ## write into master_src table
    sio = StringIO()
    sio.write(master_src.to_csv(index=None, header=None))
    sio.seek(0)
    cursor.copy_from(sio, 'master_src', columns=master_src.columns, null='', sep=',')

    ## write into backup tables
    cursor.execute('DELETE FROM master_bk WHERE snap_date=%(snap_date)s',{'snap_date':snap_date})
    cursor.execute('DELETE FROM master_src_bk WHERE snap_date=%(snap_date)s',{'snap_date':snap_date})
    master_table['snap_date']=snap_date
    master_src['snap_date']=snap_date

    sio = StringIO()
    sio.write(master_table.to_csv(index=None, header=None))
    sio.seek(0)
    cursor.copy_from(sio, 'master_bk', columns=master_table.columns, null='', sep=',')

    sio = StringIO()
    sio.write(master_src.to_csv(index=None, header=None))
    sio.seek(0)
    cursor.copy_from(sio, 'master_src_bk', columns=master_src.columns, null='', sep=',')

    ## update ucid usage
    cursor.execute('UPDATE sys_parameter SET value1=%(cur_ucid)s WHERE pname=\'ucid\';',{'cur_ucid':cur_ucid})
    connection.commit()


except (Exception, psycopg2.Error) as error:
    lineno = sys.exc_info()[-1].tb_lineno
    print('connection error: {}, in line {}'.format(error, lineno))
    connection.rollback()
finally:
    if(connection):
        cursor.close()
        connection.close()
        print('connection closed')
