import sys
import time
import pysftp as sftp
import os
import shutil
import re
import logging
from  logging import handlers

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0


def init_log(sftp_script_log_file: str):
    # --- Logging ---
    logging.basicConfig(
        format='%(asctime)s %(filename)s %(lineno)s %(levelname)-8s \n%(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    frmter = logging.Formatter('{lineno}**{message}** at{asctime}|{name}',style='{')
    logfh = handlers.RotatingFileHandler(sftp_script_log_file, mode='a', maxBytes=1024*4, 
        backupCount=0, encoding=None, delay=False, errors=None)
    logfh.setFormatter(frmter)

    log = logging.getLogger('script_sftp')
    log.addHandler(logfh)
    return log


def tstamp(format = "%Y%m%d", prefix = "", suffix = '_'):
    ret = prefix+time.strftime(format)+suffix
    return ret


def upload_file(conn, source_dir, dest_dir, fname, verbose = True):
    global log
    REG_TSTAMP = "^[0-9]{8}_"
    
    fpathname = os.path.join(source_dir, fname)
    try:
        conn.cwd(dest_dir) # non neccessario adesso
        if re.match(REG_TSTAMP, fname):
            log.warning(f"trovato file con precedente upload fallito, lo ignoro:\n{fname}")
            return
        conn.put(fpathname)
        if verbose:
            log.info(f'uploaded file: {fpathname}')
        return fpathname
    except Exception as e:
        log.error(f"fallit upload di {fpathname}")
        log.error(e)
        return None


def upload_files(source_dir, dest_dir, dir_inviati, sftp_log_file = False):

    try:
        # evita un bug interno
        cnopts = sftp.CnOpts()
        cnopts.hostkeys = None
        with sftp.Connection(host=host, username=user, password=password, log=sftp_log_file, 
        port = port, cnopts=cnopts) as sftp_conn:
            try:
                log.info(f"cd a directory remota: {dest_dir}")
                sftp_conn.cwd(dest_dir)
                log.info(f"directory remota: {sftp_conn.getcwd()}")
            except Exception as e:
                log.error(e)
                log.error(f"exiting, unable to change to remote directory: {dest_dir}")
                sys.exit(1)

            fname = ""
            try:
                # get local files list
                fnames_list = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]
                log.info(f'trovati {len(fnames_list)} files da inviare in {source_dir}')
                conteggio, errori  = 0, 0
                for fname in fnames_list:
                    conteggio += 1
                    pathname_file_uploaded = upload_file(sftp_conn, source_dir, dest_dir, fname)
                    if pathname_file_uploaded is not None:
                        fpathname_dir_inviati = os.path.join(dir_inviati, tstamp()+fname)
                        log.info(f"sposto\n{pathname_file_uploaded} a\n{fpathname_dir_inviati}")
                        shutil.move(pathname_file_uploaded, fpathname_dir_inviati)
                    else:
                        errori += 1
                log.info("#"*5+f" uploads: {conteggio} di cui falliti: {errori} "+"#"*5)
            except Exception as e:
                log.error(e)
                log.error(f"exiting, unable to upload: {fname}")
                sys.exit(1)

    except Exception as e:
        log.error(e)
        log.error(f"exiting, unable to open connection to: {host}:{port}")
        sys.exit(1)





ini_file = r"F:\00_data\GDRIVEs\enrico200165\08_dev_gdrive\configs\cfg_sftp_script\sftp.ini"

config = ConfigParser()       # instantiate
ok = config.read(ini_file)    # parse existing file
if len(config.sections()) <= 0:
    print(f'ERRORE: config file vuoto o non trovato\nfile: {ini_file}\nworking dir: {os.getcwd()}')
    sys.exit(1)

sftp_script_log_file = config.get('localhost', 'script_log_file').strip()
log = init_log(sftp_script_log_file)
log.info(f'working directory: {os.getcwd()}')
host = config.get('remotehost', 'host').strip()
log.info(f'host: {host}')
port = int(config.get('remotehost', 'port').strip())
log.info(f'port: {port}')
user = config.get('remotehost', 'user').strip()
log.info(f'user: {user}')
password = config.get('remotehost', 'password').strip()
remote_dir = config.get('remotehost', 'remote_dir').strip()
log.info(f'remote_dir: {remote_dir}')

local_source_dir = config.get('localhost', 'local_source_dir').strip()
log.info(f'local_source_dir: {local_source_dir}')
inviati_subdir = config.get('localhost', 'inviati_subdir').strip()
log.info(f'SUBdirectory per file inviati: {inviati_subdir}')


if not os.path.isdir(local_source_dir):
    log.error(f'non trovata directory sorgente: {local_source_dir}')
    sys.exit(1)

dir_inviati = os.path.join(local_source_dir, inviati_subdir)
if not os.path.isdir(dir_inviati):
    os.mkdir(dir_inviati)
upload_files(local_source_dir, remote_dir, dir_inviati, sftp_log_file = False)
