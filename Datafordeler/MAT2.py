"""
Download Datafordeler - Matriklen2
Name : DownloadDatafordelerMatriklen2
Group : ETL
With QGIS : 34000
"""

from qgis.core import ( 
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingMultiStepFeedback,
    QgsProcessingParameterString,
    QgsProcessingParameterEnum,
    QgsProcessingParameterFeatureSink,
    QgsSettings
)
import processing


class MAT2(QgsProcessingAlgorithm):
    Entity = ['BygningPaaFremmedGrundFlade','Centroide','BygningPaaFremmedGrundPunkt','Ejerlav','Ejerlejlighed','Ejerlejlighedslod','Jordstykke','Jordstykke_sekundaerForretning','JordstykkeTemaflade','Lodflade','MatrikelKommune','MatrikelRegion','Matrikelskel','MatrikelSogn','MatrikulaerSag','Nullinje','OptagetVej','SamletFastEjendom','Skelpunkt','Temalinje']
    
    def initAlgorithm(self, config=None):
        settings = QgsSettings()
        df_api_key = settings.value("kortxyz/df_api_key", "")

        self.addParameter(QgsProcessingParameterString('apikey', 'apikey', defaultValue=df_api_key, multiLine=False))
        self.addParameter(QgsProcessingParameterEnum('entity','Entity', options=self.Entity, allowMultiple=False, usesStaticStrings=True))
        self.addParameter(QgsProcessingParameterEnum('type','Type', options=['current','bitemporal','temporal'], allowMultiple=False, usesStaticStrings=True))
        self.addParameter(QgsProcessingParameterFeatureSink('Output','Output', createByDefault=True, supportsAppend=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(2, model_feedback)
        results = {}
        outputs = {}
        url = (
            "https://api.datafordeler.dk/FileDownloads/GetFile?"
            "Register=MAT"
            "&type=" + parameters["type"] +
            "&LatestTotalForEntity=" + parameters["entity"] +
            "&format=gpkg"
            "&apiKey=" + parameters["apikey"]
        )

        # Download
        alg_params = {
           'DATA':'',
           'METHOD': 0,  # GET
           'URL':url,
           'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Download'] = processing.run('native:filedownloader', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # Unzip
        alg_params = {
           'DIST':'',
           'Zipfile': outputs['Download']['OUTPUT']
        }
        outputs['Unzip'] = processing.run('KORTxyz:Unzipper', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

            
        # Extract by expression
        alg_params = {
            'EXPRESSION': '1=1',
            'INPUT': outputs['Unzip']['FIRSTFILE'],
            'OUTPUT': parameters['Output']
        }
        
        outputs['ExtractByExpression'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        return {'Output':outputs['ExtractByExpression']['OUTPUT']}

    def name(self):
        return'DownloadDatafordelerMatriklen'

    def displayName(self):
        return'Download Matriklen'

    def group(self):
        return'Datafordeler'

    def groupId(self):
        return'Datafordeler'

    def createInstance(self):
        return MAT2()
