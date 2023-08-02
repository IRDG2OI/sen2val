# source : https://sftptogo.com/blog/python-sftp/

import os  # file system operations
from stat import S_ISDIR
from paramiko import Transport, SFTPClient  # distant connection


class Sftp:
    '''
    ------------------------------
    Constructor Method
    ------------------------------
    '''
    def __init__(self, hostname:str, username:str, password:str, port:int=22):
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port

    ''' 
    ------------------------------
    Connects to the sftp server and returns the sftp connection object 
    ------------------------------
    '''
    def connect(self):

        try:
            # Create Transport object using supplied method of authentication.
            transport = Transport(sock=(self.hostname, self.port))
            transport.connect(None, self.username, self.password, None)

            # Get the sftp connection object
            self.connection = SFTPClient.from_transport(transport)

            print(f'Connected to {self.hostname} as {self.username}.')

        except Exception as err:
            if self.connection:
                self.connection.close()
            if transport:
                transport.close()
            print('An error occurred creating SFTP client: %s: %s' % (err.__class__, err))
            raise Exception(err)
        pass

    ''' 
    ------------------------------
    Closes the sftp connection
    ------------------------------
    '''
    def disconnect(self):
        self.connection.close()
        print(f'Disconnected from host {self.hostname}')

    ''' 
    ------------------------------
    Lists all the files and directories in the specified path and returns them
    ------------------------------
    '''
    def listdir(self, remote_path:str):
        for obj in self.connection.listdir(remote_path):
            yield obj

    ''' 
    ------------------------------
    Prints files of SFTP location
    ------------------------------
    '''
    def print_listdir(self, remote_path: str):
        print(f'List of files at location {remote_path}:')
        print([f for f in self.connection.listdir(remote_path)])


# --TODO-- optimiser le parcours des fichiers avec la regex.
#    Là, parcourue une fois par listdir_attr + test sur chaque item par la regex. Faire le filtre dès list_dir
    def download_folder(self, remote_path: str, dest_path: str, limit_download_size: int, ext: str = '', limit_nb_subrep: int = 1300, force: bool = False):

        # --TODO--
        #    vérifier la longueur de 'ext' et renvoyer une erreur. si vide, prendre tous les fichiers

        # print('Browsing ', remote_path)

        item_list = self.connection.listdir_attr(remote_path)
        item_list_len = len(item_list)
        dest_path = str(dest_path)
        i = 0

        if limit_nb_subrep >= 0 and limit_download_size >= 0:
            try:
                os.makedirs(dest_path, exist_ok=True)
                if not os.path.isdir(dest_path):
                    print('Creating download repository ', dest_path)
            except Exception as err:
                print('An error occurred while creating download repository $s:\n $s: $s' % (
                dest_path, err.__class__, err))
                raise Exception(err)
            pass
            while i < item_list_len and limit_download_size >= 0 and limit_nb_subrep >= 0:
                mode = item_list[i].st_mode
                '''if there is a folder to explore, recursive call'''
                if S_ISDIR(mode):
                    limit_nb_subrep -= 1
                    # print('Subrepo number left : ', limit_nb_subrep)
                    limit_download_size = self.download_folder(remote_path + item_list[i].filename + '/', dest_path + item_list[i].filename + '/', limit_download_size, ext, limit_nb_subrep, force)
                else:
                    try:
                        '''if there is a file'''
                        if ext:
                            '''select only files with the given extension'''
                            # --TODO--
                            #    improve file extension selection (regex with '.' character)
                            if item_list[i].filename[-4:] == ext:
                                '''if the file does not exist   or  (if there is already an existing file in download folder    and     wrinting is forced)'''
                                if not os.path.exists(dest_path + item_list[i].filename) or (os.path.exists(dest_path + item_list[i].filename) and force):
                                        print('Downloading : ', remote_path + item_list[i].filename)
                                        self.connection.get(remote_path + item_list[i].filename,
                                                            dest_path + item_list[i].filename)
                                        limit_download_size -= item_list[i].st_size
                                        # print('Download size left : ', limit_download_size)
                                else:
                                    print('Skip download. File already exists : ', dest_path + item_list[i].filename)

                            '''if the folder is empty or has no matching file'''
                        else:
                            if not os.path.isdir(dest_path + item_list[i].filename):
                                print('Downloading : ', remote_path + item_list[i].filename)
                                self.connection.get(remote_path + item_list[i].filename, dest_path + item_list[i].filename)
                                limit_download_size -= item_list[i].st_size
                                # print('Download size left : ', limit_download_size)

                    except Exception as err:
                        print('An error occurred while downloading $s/$s :\n %s: %s' % (remote_path, item_list[i].filename, err.__class__, err))
                        raise Exception(err)
                i += 1
        else:
            if limit_nb_subrep < 0:
                print('Stop uploading. Number of repositories limit reached.')
            else:
                print('Stop uploading. Download size limit reached.')

        return limit_download_size
