"""
Call a FTP server from QGIS
Name : ftpCaller
Group : ETL
With QGIS : 34000
"""

from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingParameterString
from qgis.core import QgsProcessingOutputString
from qgis.core import QgsProcessingParameterFeatureSink
from qgis.core import QgsProcessingOutputVariant
from qgis.core import QgsProcessingUtils
from qgis.core import QgsMessageLog

import processing

from ftplib import FTP_TLS, FTP, all_errors
from urllib.parse import urlparse

import ssl
import os


class ImplicitFTP_TLS(FTP_TLS):
    """FTP_TLS subclass to support implicit FTPS."""
    """Constructor takes a boolean parameter ignore_PASV_host whether o ignore the hostname"""
    """in the PASV response, and use the hostname from the session instead"""
    def __init__(self, *args, **kwargs):
        self.ignore_PASV_host = kwargs.get('ignore_PASV_host') == True
        super().__init__(*args, {k: v for k, v in kwargs.items() if not k == 'ignore_PASV_host'})
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def ntransfercmd(self, cmd, rest=None):
        """Override the ntransfercmd method"""
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        conn = self.sock.context.wrap_socket(
            conn, server_hostname=self.host, session=self.sock.session
        )
        return conn, size        
     
    def makepasv(self):
        host, port = super().makepasv()
        return (self.host if self.ignore_PASV_host else host), port      
        

class FTPcaller(QgsProcessingAlgorithm):
    
    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterString(
                'HOST',
                'The host to connect to'
            )
        )
        
        self.addParameter(
            QgsProcessingParameterString(
                'USER',
                'Username to connect with'
            )
        )
        
        self.addParameter(
            QgsProcessingParameterString(
                'PASSWD',
                'The password to connect with'
            )
        )
        
        self.addOutput(
            QgsProcessingOutputVariant(
                'LIST',
                'List of content in dir'
            )
        )
        
        self.addOutput(
            QgsProcessingOutputString(
                'FILEPATH',
                'Filepath from FTP'
            )
        )
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                'FILE',
                'File from FTP'
            )
        )
    
    def processAlgorithm(self, parameters, context, model_feedback):
        results = {}
        outputs = {}
        
        host = self.parameterAsString(parameters, 'HOST', context)
        user = self.parameterAsString(parameters, 'USER', context)
        passwd = self.parameterAsString(parameters, 'PASSWD', context)
        
        scheme,netloc,path, *rest = urlparse(host)

        if scheme == 'ftps':
            ftp = ImplicitFTP_TLS()
            ftp.connect(host=netloc, port=990)
        else:
            ftp = FTP(netloc)
            
        ftp.login(user=user, passwd=passwd)
        
        ftp.voidcmd('TYPE I')
        
        try:
            tempfilepath = os.path.join(
                QgsProcessingUtils.tempFolder(),
                path.split('/')[-1]
            )
            
            file = open(tempfilepath, 'wb')
            ftp.retrbinary('RETR '+ path, file.write)
            
            file.close()
            
            results['FILEPATH'] = tempfilepath
            results['FILE'] = tempfilepath

        except all_errors as e:
            QgsMessageLog.logMessage(str(e),'error')
            list = ftp.nlst(path)

            results['LIST'] =  [(scheme + "://"+netloc+e) for e in list]
            
        ftp.close()
        
        return results

    def name(self):
        return 'FTPcaller'

    def displayName(self):
        return 'FTPcaller'

    def group(self):
        return 'ETL'

    def groupId(self):
        return 'ETL'

    def shortHelpString(self):
        return "Call a FTP server from QGIS"

    def createInstance(self):
        return FTPcaller()