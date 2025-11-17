"""
Unzip file
Name : Unzipper
Group : ETL
With QGIS : 34000
"""
import os


from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingParameterFile
from qgis.core import QgsProcessingOutputFile
from qgis.core import QgsProcessingOutputVariant
from qgis.core import QgsProcessingUtils
from qgis.core import QgsZipUtils

import processing


class Unzipper(QgsProcessingAlgorithm):
    
    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterFile('Zipfile','File to be unzipped', behavior=QgsProcessingParameterFile.File )
        )
        
        self.addParameter(
            QgsProcessingParameterFile('DIST', 'Destination to unzip files to', optional=True, behavior=QgsProcessingParameterFile.Folder, defaultValue=None)
        )
        
        self.addOutput(
            QgsProcessingOutputVariant('DISTFILES', 'List of unzipped files')
        )

        self.addOutput(
            QgsProcessingOutputFile('FIRSTFILE', 'First file in Zip')
        )


    def processAlgorithm(self, parameters, context, feedback):
        results = {}
        outputs = {}
        
        if not parameters['DIST']:
            parameters['DIST'] = QgsProcessingUtils.tempFolder()
   
        outputs['UnzippedFiles'] = QgsZipUtils.unzip(parameters['Zipfile'],parameters['DIST'])
        
        
        results['DISTFILES'] = outputs['UnzippedFiles'][1]
        results['FIRSTFILE'] = outputs['UnzippedFiles'][1][0]
        
        return results

    def name(self):
        return 'Unzipper'

    def displayName(self):
        return 'Unzipper'

    def group(self):
        return 'ETL'

    def groupId(self):
        return 'ETL'

    def shortHelpString(self):
        return "Unzip file"

    def createInstance(self):
        return Unzipper()
