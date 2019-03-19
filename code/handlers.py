import os
import subprocess
import logging as l
from getpass import getuser
from hdfs.ext.kerberos import KerberosClient
from hdfs.util import HdfsError

def _byte_to_human_readable(size):
    """Convert a number of bytes to a human-readable form"""
    power = 1024.
    n = 0
    labels = {0 : 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power:
        size /=  power
        n += 1
    return str(round(size,1))+labels[n]

class QueenBee(object):
    """A selection of useful functions for dealing with 
    Hive and spark parquet files
    
    Base class for other CUPS classes"""
    
    def __init__(self, cwd=None, client_url=None):
        if not cwd:
            cwd = self.homedir
        if cwd[-1] != '/':
            cwd += '/'
        self._cwd = cwd
        try:
            self.client = KerberosClient(client_url, mutual_auth='DISABLED')
            fl = self.client.list(hdfs_path=cwd)
        except HdfsError:
            raise ValueError("Client cannot be found, please supply namenode URL")
        except:
            raise
        return
        
    def __repr__(self):
        return "QueenBee(cwd='{}')".format(self._cwd)
    
    def _get_absolute_path(self, target):
        n_up = target.count('../')
        if n_up > 0:
            # it's a relative path up from cwd
            dirs = self._cwd.split('/')
            if self._cwd[-1] == '/':
                dirs = dirs[:-1]
            parent = dirs[:len(dirs)-n_up]
            parent = '/'.join(parent)
            path = parent+'/'+target.replace('../','')
        else:
            if target[0] == '/':
                # it's root directory so just take target
                path = target
            else:
                # it's a relative path from cwd
                path = self._cwd+target
        return path
    
    def _call_hdfs(self, *args):
        return subprocess.check_output(["hdfs", "dfs"]+list(args))

    @property
    def currentdir(self):
        return self._cwd
    
    @currentdir.setter
    def currentdir(self, val):
        raise AttributeError('Direct path setting not supported, '
                             'use chdir() instead')
        return None
    
    @property
    def homedir(self):
        return '/user/'+getuser()+'/'
        
    def path_exists(self, path):
        """Check if HDFS path exists"""
        path = self._get_absolute_path(path)
        try:
            self.client.status(path)
        except HdfsError:
            return False
        except:
            raise
        return True
        
    def is_dir(self, path):
        """Determines if the path is a directory or a file"""
        path = self._get_absolute_path
        status = self.client.status(path)
        if status['type'] == 'DIRECTORY':
            return True
        return False
        
    def chdir(self, target):
        """Change working directory to target path"""
        self._cwd = self._get_absolute_path(target)
        if self._cwd[-1] != '/':
            self._cwd += '/'
        return

    def mv_to_local(self, file, target, **kwargs):
        """Copy HDFS file to local directory target. See
        HDFS client documentation for more kwargs."""
        path = self._get_absolute_path(file)
        if self.path_exists(path):
            self.client.download(path, target, **kwargs)
        return
        
    def mv_to_hive(self, file, target, **kwargs):
        """Copy local file or directory to target HDFS directory.
        See HDFS client documentation for more kwargs."""
        path = self._get_absolute_path(target)
        self.client.upload(path, file, **kwargs)
        return
        
    def ls(self, path=None):
        """Returns names of files in remote folder"""
        if not path:
            path = self._cwd
        path = self._get_absolute_path(path)
        files = self.client.list(path)
        return files
    
    def mkdir(self, path, permission=None):
        """Make the specified directory, recursively if needed"""
        path = self._get_absolute_path(path)
        self.client.makedirs(path, permission)
        return
    
    def delete(self, file, recursive=False):
        """Delete the specified file or directory. Recursive option will 
        delete all files in directory if enabled."""
        file = self._get_absolute_path(file)
        if not self.path_exists(file):
            return
        self.client.delete(file, recursive)
        return
    
    def file_size(self, file):
        """Find the size in bytes of the specified file
        
        Returns: 
            - dict: 'actual': size of file on disk,
                    'replicated': size of file on disk after HDFS duplication"""
        file = self._get_absolute_path(file)
        status = self.client.status(file)
        return {'actual':status['length'], 
                'replicated':status['length'] * status['replication']}
    
    def rename(self, file, target, overwrite=False, fill_missing=False):
        """Rename (or move) file to target file/directory.
        If overwrite is enabled, deletes existing target path
        first then moves file.
        If fill_missing is enabled, creates target path if it doesn't
        already exist."""
        curr_path = self._get_absolute_path(file)
        new_path = self._get_absolute_path(target)
        if overwrite:
            if self.path_exists(new_path) and not self.is_dir(new_path):
                self.delete(new_path)
        if fill_missing:
            path = new_path
            if not self.path_exists(path) and self.is_dir(path):
                self.mkdir(path)
            elif not self.path_exists(path):
                path = path[:path.rfind('/')]
                self.mkdir(path)
        self.client.rename(curr_path, new_path)
        return