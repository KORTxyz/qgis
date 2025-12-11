from qgis.core import QgsProcessingProvider
from qgis.PyQt.QtGui import QIcon
import os

from .ETL.ftp_caller import FTPcaller
from .ETL.unzipper import Unzipper

from .Datafordeler.DAGI import DAGI
from .Datafordeler.GeoDK import GeoDK
from .Datafordeler.MAT2 import MAT2
from .Datafordeler.Stednavne import Stednavne


class KORTxyzProvider(QgsProcessingProvider):

    def __init__(self):
        super().__init__()

    def loadAlgorithms(self):
        self.addAlgorithm(FTPcaller())
        self.addAlgorithm(Unzipper())

        self.addAlgorithm(DAGI())
        self.addAlgorithm(GeoDK())
        self.addAlgorithm(MAT2())
        self.addAlgorithm(Stednavne())

    def id(self):
        return 'KORTxyz'

    def name(self):
        return self.tr('KORTxyz')

    def icon(self):
        icon_path = os.path.join(os.path.dirname(__file__), "icon.png")
        return QIcon(icon_path)

    def longName(self):
        return self.name()
