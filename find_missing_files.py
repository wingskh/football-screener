import os
import datetime

def find_missing_pkl_files(start_date, end_date, folder_path):
    start_date = datetime.datetime.strptime(start_date, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_date, '%Y%m%d')

    missing_files = []
    current_date = start_date

    while current_date <= end_date:
        file_name = current_date.strftime('%Y%m%d') + '.pkl'
        file_path = os.path.join(folder_path, file_name)
        print(f'{file_path}: {os.path.exists(file_path)}')
        if not os.path.exists(file_path):
            missing_files.append(file_name)

        current_date += datetime.timedelta(days=1)

    return missing_files

folder_path = '/Users/wingsun/Downloads/FYP2.0/'
start_date = "20191223"
end_date = "20231030"
find_missing_pkl_files(start_date, end_date, folder_path)
