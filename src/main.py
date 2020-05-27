import csv
import re
import os
import sys
from contextlib import contextmanager
from io import StringIO
import logging

import paramiko
from logstash_formatter import LogstashFormatterV1
from keboola import docker


@contextmanager
def sftp_connection(server_address, port_number, username, password_con, rsa, passphrase_key):
    logger.info('Establishing sftp connection.')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key(StringIO(rsa), password=passphrase_key)
    sftp_con = None
    try:
        ssh.connect(server_address, port=port_number, username=username,
                    password=password_con, pkey=pkey)
        sftp_con = ssh.open_sftp()
        yield sftp_con
    except Exception as exc:
        logger.error(f'Failed to establish SFTP connection. Exception {exc}')
    finally:
        if sftp_con is not None:
            logger.info('Closing sftp.')
            sftp_con.close()
        logger.info('Closing ssh.')
        ssh.close()


def process_line(line, **kwargs):
    processed_eshops = []
    results = []
    # the order is important
    # if highlighted, we want to preserve the info adn ignore other records for the same shop
    for mapping in HIGHLIGHTED_FIELDS + OBSERVED_FIELDS + CHEAPEST_FIELDS + MALL_FIELDS:
        full_mapping = {**COMMON_FIELDS, **mapping}
        shop_data = {
            full_mapping[key]: line[key]
            for key in full_mapping.keys()
        }
        if mapping == MALL_FIELDS[0]:
            shop_data['ESHOP'] = 'mall.hu'
            shop_data['AVAILABILITY'] = ''
        if shop_data['ESHOP'] != '' and shop_data['ESHOP'] not in processed_eshops:
            if 'Highlighted' in list(mapping.keys())[0]:
                shop_data['HIGHLIGHTED_POSITION'] = re.findall(
                    r'\d+',
                    list(mapping.keys())[0])[0]
            shop_data[
                'STOCK'] = 1 if shop_data['AVAILABILITY'] == 'instock' else 0
            shop_data['TS'] = kwargs['file_timestamp']
            shop_data['SOURCE_ID'] = kwargs['filename']
            shop_result = {**CONSTANT_FIELDS, **shop_data}
            processed_eshops.append(shop_result['ESHOP'])
            results.append(shop_result)
    return results


def get_file_dicts(filepath):
    name = filepath.split('/')[-1]
    with open(filepath, 'r') as fl:
        # data for shops start at the second line
        timestamp = fl.readline().strip('\n')
        reader = csv.DictReader(fl, delimiter=';')
        for line in reader:
            line_dicts = process_line(line,
                                      file_timestamp=timestamp,
                                      filename=name)
            yield line_dicts


if __name__ == '__main__':
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = LogstashFormatterV1()

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level='DEBUG')

    datadir = os.getenv('KBC_DATADIR', '/data/')
    cfg = docker.Config(datadir)
    parameters = cfg.get_parameters()

    # log parameters (excluding sensitive designated by '#')
    logger.info({k: v for k, v in parameters.items() if "#" not in k})

    previous_timestamp_filename = parameters.get('previous_timestamp_filename')
    filename_pattern = parameters.get('filename_pattern')
    server = parameters.get('server')
    port = int(parameters.get('port'))
    user = parameters.get('username')
    password = parameters.get('#password')
    passphrase = parameters.get('#passphrase')
    rsa_key = parameters.get('#key')
    wanted_columns = parameters.get('wanted_columns')

    SFTP_FOLDER = '/upload/'

    COMMON_FIELDS = {
        'ItemCode': 'MATERIAL',
        'EAN': 'EAN',
        'AKIdentifier': 'CSE_ID',
        'AKCategoryName': 'CATEGORY_NAME',
        'Rating': 'RATING',
        'ReviewCount': 'REVIEW_COUNT'
    }

    HIGHLIGHTED_FIELDS = [{
        f'Highlighted{i} EshopName': 'ESHOP',
        f'Highlighted{i} Price': 'PRICE',
        f'Highlighted{i} Stock': 'AVAILABILITY',
        f'Highlighted{i} ShippingPrice': 'SHIPPING_PRICE'
    } for i in range(1, 4)]

    OBSERVED_FIELDS = [{
        f'Observed{i} Name': 'ESHOP',
        f'Observed{i} Price': 'PRICE',
        f'Observed{i} Stock': 'AVAILABILITY',
        f'Observed{i} ShippingPrice': 'SHIPPING_PRICE'
    } for i in range(1, 6)]

    CHEAPEST_FIELDS = [{
        'Cheapest EshopName': 'ESHOP',
        'Cheapest Price': 'PRICE',
        'Cheapest Stock': 'AVAILABILITY',
        'Cheapest ShippingPrice': 'SHIPPING_PRICE'
    }]

    MALL_FIELDS = [{'Price': 'PRICE', 'Position': 'POSITION'}]

    CONSTANT_FIELDS = {'COUNTRY': 'HU', 'DISTRCHAN': 'MA', 'SOURCE': 'arukereso', 'FREQ': 'd'}

    with open(f'{datadir}in/tables/{previous_timestamp_filename}') as input_file:
        previous_timestamp_list = [
            str(ts.replace('"', ''))
            for ts
            # read all input file rows, except the header
            in input_file.read().split(os.linesep)[1:]
        ]
        previous_timestamp = float(previous_timestamp_list[0])

    last_timestamp = previous_timestamp
    files_to_process = []

    destroot = f'{datadir}in/tables/downloaded_csvs'
    if not os.path.exists(destroot):
        os.makedirs(destroot)

    # NB: original script downloaded both from upload and upload/archive
    # archive seems to contain only records that are several days old
    folder = SFTP_FOLDER
    with sftp_connection(server, port, user, password, rsa_key, passphrase) as sftp:
        for file in sftp.listdir_attr(folder):
            modified_time = file.st_mtime
            if (modified_time > previous_timestamp) and file.filename.startswith(filename_pattern):
                if modified_time > last_timestamp:
                    last_timestamp = modified_time
                sourcepath = f'{folder}{file.filename}'
                logger.info(f'Downloading file {sourcepath}')
                destpath = f'{destroot}/{file.filename}'
                files_to_process.append(destpath)
                sftp.get(sourcepath, destpath)

    if not files_to_process:
        logger.info('No new files to process. Exiting.')
        sys.exit(0)

    logger.info(f'Downloaded {len(files_to_process)} files.')

    with open(f'{datadir}out/tables/results.csv', 'w+') as f:
        writer = csv.DictWriter(f, fieldnames=wanted_columns, extrasaction='ignore')
        writer.writeheader()
        for file in files_to_process:
            logger.info(f'Processing file: {file}')
            try:
                for result_dicts in get_file_dicts(file):
                    writer.writerows(result_dicts)
            except Exception as e:
                logger.error(f'Failed to proces file: {file}. Exception {e}.')

    logger.info('Processing done. Writing last timestamp.')
    with open(f'{datadir}out/tables/arukereso_last_timestamp.csv', 'w+') as fo:
        dict_writer = csv.DictWriter(fo, fieldnames=['max_timestamp_this_run'])
        dict_writer.writeheader()
        dict_writer.writerow({'max_timestamp_this_run': last_timestamp})
