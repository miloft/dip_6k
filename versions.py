from datetime import datetime
import sys


if len(sys.argv) == 2:
    with open(sys.argv[1]) as logfile:
        logs = logfile.readlines()
        if logs:
            if logs[-1] == '\n':
                logs.pop()
            with open("add_proc_file.sql", 'w') as sql:
                sql_array = []
                sql_array.append(f"INSERT processing(data_name, processing_type, dt_result, dt_processing, state, version) VALUES\n")
                for line in logs:
                    params = line.split(' ')
                    if len(params) == 6:
                        data_name = params[0]
                        versions = params[1]
                        type = params[2]
                        state = params[3]
                        dt = params[4] + ' ' + params[5][:-8]
                        sql_array.append(f"('{data_name}', '{type}', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{dt}', "
                                  f"'{state}', '{versions}')")
                        if line != logs[-1]:
                            sql_array.append(",\n")
                        else:
                            sql_array.append(";")
                    else:
                        print(f'Found: {line}\n'
                              f'Invalid record format. Expect:\n'
                              f'<input_file> <new_file> <type_of_processing> '
                              f'<state[success|fail]> <datetime_of_processing>\n'
                              f'Parameters must be separated by only 1 space.')
                        sql_array.clear()
                        break
                if len(sql_array):
                    sql.writelines(sql_array)
                    sql.write('\n')
                    sql.write(f'insert processing_data(data_id, processing_id)\n'
                      f'select ID, processing_id\n'
                      f'from data right outer join (\n'
                      f'select processing_id, data_name from processing\n'
                      f'order by processing_id desc limit {len(logs)})\n'
                      f'proc on data.file_NAME = proc.data_name')
                    with open("args", 'w'):
                        pass
        else:
            print('File is empty')
else:
    print(f'usage: ./versions.py <log_file>\n'
          f'<log_file> -  file of lines corresponding to processed records:\n'
          f'<input_file> <new_file> <type_of_processing> <state[success|fail]> <datetime_of_processing>')
