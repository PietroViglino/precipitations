import requests
import gzip
from io import BytesIO
import logging
import os

def noaa_to_csv(output_folder=''):
    """This function takes the stations' informations from the txt file 'ghcnd-stations.txt',
    selects only the rows in which the latitude is < than -40 and then uses the stations' names
    to make a request to noaa's API and get the data. The data is then elaborated and
    used to generate csv files."""
    lines_formatted = []
    with open('ghcnd-stations.txt', 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.split(' ')
            line = [_.replace('\n', '') for _ in line if _ != '' and _ != ' ' and _ != '\n' and _ != 'GSN']
            if not line[-1][0].isdigit() or '(' in line[-1] or ')' in line[-1]:
                line.append('null')
            counter = 4
            while counter < len(line) - 2:
                line[counter + 1] = line[counter] + '_' + line[counter + 1]
                line.pop(counter)
            assert len(line) == 6, 'Lenght of line list should be 6'
            if float(line[1]) < -40:
                lines_formatted.append(line)
    lines_formatted = sorted(lines_formatted, key=lambda x : x[0][:2])
    logging.debug(f'Number of stations: {len(lines_formatted)}')
    for index, line in enumerate(lines_formatted):
        station_name = line[0]
        lat = line[1]
        long = line[2]
        elevation = line[3]
        wmo = line[5]
        logging.info(f'Start data retrieval and elaboration for {station_name}')
        logging.debug(f'Getting data for {station_name}')
        response = requests.get(f'https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/{station_name}.csv.gz')
        if response.status_code == 200:
            compressed_content = BytesIO(response.content)
            with gzip.open(compressed_content, 'rt') as f:
                logging.debug(f'Received data for {station_name}. Working...')
                csv_lines = f.readlines()
                times_list = []
                splitted_lines = []
                for line in csv_lines:
                    line = line.split(',') 
                    times_list.append(line[1])
                    splitted_lines.append(line)
                if f'{station_name}_{wmo}_{lat}_{long}_{elevation}.csv' in os.listdir(output_folder+'/'):  
                    with open(f'{output_folder}/{station_name}_{wmo}_{lat}_{long}_{elevation}.csv', 'r') as f:
                        lines = f.readlines()
                        if len(lines) > 0:
                            logging.info(f'Skipping {station_name}_{wmo}_{lat}_{long}_{elevation}.csv')
                            percentage = ((index + 1) / len(lines_formatted)) * 100
                            logging.debug(f'Job at {percentage}%')
                            continue                        
                with open(f'{output_folder}/{station_name}_{wmo}_{lat}_{long}_{elevation}.csv', 'w') as output_file:
                    times_list = list(set(times_list))
                    result = []
                    for time in times_list:
                        dict_for_time = {"TIME": time, "PRCP": "null", "TAVG": "null", "SNOW": "null"}
                        for line in splitted_lines:
                            if time in line:
                                if line[2] == 'PRCP':
                                    dict_for_time['PRCP'] = line[3]
                                if line[2] == 'TAVG':
                                    dict_for_time['TAVG'] = line[3]
                                if line[2] == 'SNOW':
                                    dict_for_time['SNOW'] = line[3]
                        result.append(dict_for_time)
                    logging.debug('Data elaboration completed. Now writing CSV file...')
                    result = sorted(result, key=lambda x : x['TIME'])
                    for line in result:
                        time_formatted = line['TIME'][:4] + '-' + line['TIME'][4:6] + '-' + line['TIME'][6:8] + 'T00:00:00Z'
                        csv_line = f"{time_formatted},{line['PRCP']},{line['TAVG']},{line['SNOW']}\n"
                        output_file.write(csv_line)
                percentage = ((index + 1) / len(lines_formatted)) * 100
                logging.debug(f'Job at {percentage}%')
                logging.info(f'Finished elaborating data and writing CSV for {station_name}')
        else:
            logging.error(f'Failed data retrieval for {station_name}')

if __name__ == '__main__':  
    logger = logging.getLogger()    
    file_handler = logging.FileHandler(filename="logging/logfile.log", mode="w")
    formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG) 
    noaa_to_csv('output')
