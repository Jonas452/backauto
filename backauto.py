import os
import glob
import pysftp
import datetime
import json
import math

class BackAuto:

    log_path = ''
    log_text = ''

    file_extensions = None
    servers = None

    successes = 0
    errors = 0
    files_kb = 0
    
    def __init__(self):
        self.add_log('<< Initializing BackAuto Script >>')
        if self.load_config():
            self.copy_backup_files()
            self.show_final_info()
            
        self.add_log('')
        self.add_log('<< Ending BackAuto Script >>')
        self.write_log()

    def load_config(self):
        """ Get the configurations from a JSON file and set in the propers variables

        Returns:
        bool: If success loading the log.

        """
        try:
            self.add_log('')
            self.add_log('Loading configurations')

            json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
            with open(json_path) as json_file:
                data = json.load(json_file)
                self.log_path = data['log_path']
                self.file_extensions = data['file_extensions']
                self.servers = data['servers']
            self.add_log('Configurations loaded with success')
            return True
        except Exception as e:
            self.add_log('[ERROR] an error occurred while trying to load the configuration file')
            self.add_log(str(e))
            return False

    def copy_backup_files(self):
        ''' Copy all the files from the backup server '''
        if self.servers is not None:
            i = 1
            for server in self.servers:
                self.add_log('')
                self.add_log('Importing from server ' + str(i) + '/' + str(len(self.servers)) + ' [' + server['host'] + ']')

                last_backuped_date = self.get_last_backuped_date(server['storage_path'], server['file_base_name'])

                ftp = self.connect_to_server(server)
                if ftp is not None:
                    files = self.clean_files_list(server['file_base_name'], self.get_server_files(ftp))
                    if len(files) > 0:
                        self.add_log('Saving files at [' + server['storage_path'] + ']')
                        for file in files:
                            date_filename = self.get_formated_file_date(file.filename)
                            if date_filename is not None:
                                if date_filename > last_backuped_date:
                                    self.add_log('Downloading the file [' + file.filename + '] of size ' + self.convert_size(file.st_size))
                                    downloaded = self.download_file(ftp, server['backup_path'] + file.filename, server['storage_path'] + file.filename)

                                    if downloaded:
                                        self.files_kb += file.st_size
                                        self.successes += 1
                                    else:
                                        self.errors += 1
                    else:
                        self.add_log('No files found to backup')
                    ftp.close()

                i += 1
        else:
            self.add_log('No servers found in the configuration file')

    def get_last_backuped_date(self, storage_path, file_base_name):
        """ Search for the last file stored at the storage_path

        Returns:
        datetime: The datetime of the last stored file.
        
        """
        list_of_files = glob.glob(storage_path + os.sep + '*.sql')
        latest_file = max(list_of_files, key=os.path.getctime)
        latest_file = latest_file.replace('.sql', '')

        return datetime.datetime.strptime(latest_file[-8:], '%d%m%Y')

    def connect_to_server(self, server):
        """ Try to connects to the the server using SFTP

        Returns:
        ftp: The ftp connection

        """
        try:
            self.add_log('Trying to establish a connection with the server')
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None

            ftp = pysftp.Connection(host=server['host'], username=server['username'], password=server['password'], port=22, cnopts=cnopts)
            self.add_log('Connection established with success')
            ftp.cwd(server['backup_path'])
            return ftp
        except Exception as e:
            self.add_log('[ERROR] an error occurred while trying to connect to the server:')
            self.add_log(str(e))
            return None
        
    def get_server_files(self, ftp):
        """ Get a list of files located in the server

        Returns:
        dir: A list with the files of the folder in the server

        """
        try:
            return ftp.listdir_attr()
        except Exception as e:
            self.add_log('[ERROR] an error occurred while trying to list the server backup files:')
            self.add_log(str(e))
            return None

    def clean_files_list(self, file_base_name, files):
        """ Get files online ending in .tar.gz or .sql

        Returns:
        list: List of the files ending in .tar.gz or .sql

        """
        cleaned_files = []
        if files is not None:
            for file in files:
                if file.filename.find(file_base_name) != -1 and (file.filename.find('.tar.gz') != -1 or file.filename.find('.sql') != -1):
                    cleaned_files.append(file)

        return cleaned_files

    def get_formated_file_date(self, filename):
        """ Gets the date from the name of the file

        Returns:
        datetime: date extracted from the file name.

        """
        try:
            temp_filename = filename.replace('.sql', '')
            temp_filename = temp_filename.replace('.tar.gz', '')

            return datetime.datetime.strptime(temp_filename[-8:], '%d%m%Y')
        except Exception:
            self.add_log('File not properly formated [' + filename + '], thus, ignored')
            return None

    def download_file(self, sftp, remoteFilePath, localFilePath ):
        """ Download a file from a ftp to the local path.

        Returns:
        bool: If the process was executed with success.

        """
        try:
            sftp.get(remoteFilePath, localFilePath)
            self.add_log('File downloaded with success')
            return True
        except Exception as e:
            self.add_log('[ERROR] an error occurred while trying to download the file:')
            self.add_log(str(e))
            return False

    def delete_old_backups(self):
        #TODO Delete old backup from FTP, and also from disk
        pass

    def show_final_info(self):
        ''' Adds to the log the information of the execution '''
        self.add_log('')
        self.add_log('Imported: ' + self.convert_size(self.files_kb))
        self.add_log('Successes: ' + str(self.successes))
        self.add_log('Errors: ' + str(self.errors))

    def convert_size(self, size_bytes):
        """ Convert the a file size to the biggest possible

        Returns:
        str: The file size with the actual size and the size name

        """
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def add_log(self, text):
        """ Add a string to the log file

        Parameters:
        text (str): The string which going to be added.

        """
        if text == '':
            print('')
            self.log_text += os.linesep
        else:
            print(self.get_current_datetime() + " --- " + text)
            self.log_text += self.get_current_datetime() + " --- " + text + "\n"

    def write_log(self):
        ''' Write the log to a txt file ''' 
        log_file_name = 'backauto_' + str(datetime.datetime.now().strftime('%Y-%m-%d')) + '.txt'
        log_file_path = os.path.join(self.log_path, log_file_name)

        file = open(log_file_path, 'a')
        file.write(self.log_text)
        file.close()

    def get_current_datetime(self):
        """Current datetime formated 

        Returns:
        str: datetime formated
        
        """
        return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

BackAuto()
