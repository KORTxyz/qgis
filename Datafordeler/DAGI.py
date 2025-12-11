"""
Download Datafordeler - DAGI
Name : DownloadDatafordelerDagi
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


class DAGI(QgsProcessingAlgorithm):

    ENTITIES = [
        'Sogneinddeling', 'Samlepostnummer', 'Retskreds', 'Regionsinddeling',
        'Postnummerinddeling', 'Politikreds', 'Opstillingskreds',
        'MRafstemningsomraade', 'Landsdel', 'Kommuneinddeling', 'Danmark',
        'Afstemningsomraade', 'Storkreds', 'SupplerendeBynavn', 'Valglandsdel'
    ]

    TYPES = ['current', 'bitemporal', 'temporal']

    # -------------------------
    # INIT PARAMETERS
    # -------------------------
    def initAlgorithm(self, config=None):
        settings = QgsSettings()
        df_api_key = settings.value("kortxyz/df_api_key", "")

        self.addParameter(
            QgsProcessingParameterString(
                'apikey',
                'Datafordeler API key',
                defaultValue=df_api_key,
                multiLine=False
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                'entity',
                'Entity',
                options=self.ENTITIES,
                allowMultiple=False
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                'type',
                'Dataset type',
                options=self.TYPES,
                allowMultiple=False
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSink(
                'Output',
                'Output layer'
            )
        )

    # -------------------------
    # MAIN PROCESSING
    # -------------------------
    def processAlgorithm(self, parameters, context, model_feedback):

        feedback = QgsProcessingMultiStepFeedback(3, model_feedback)

        # Build URL safely
        entity = self.ENTITIES[parameters['entity']]
        dtype = self.TYPES[parameters['type']]
        apikey = parameters['apikey']

        url = (
            f"https://api.datafordeler.dk/FileDownloads/GetFile"
            f"?Register=DAGI"
            f"&type={dtype}"
            f"&LatestTotalForEntity={entity}"
            f"&format=gpkg"
            f"&apiKey={apikey}"
        )

        feedback.pushInfo(f"Requesting: {url}")

        # STEP 1 – Download
        alg_params = {
            'DATA': '',
            'METHOD': 0,  # GET
            'URL': url,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

        outputs = {}
        outputs['Download'] = processing.run(
            'native:filedownloader',
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True
        )

        if feedback.isCanceled():
            return {}

        feedback.setCurrentStep(1)

        # STEP 2 – Unzip
        alg_params = {
            'DIST': '',
            'Zipfile': outputs['Download']['OUTPUT']
        }

        outputs['Unzip'] = processing.run(
            'KORTxyz:Unzipper',
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True
        )

        if feedback.isCanceled():
            return {}

        feedback.setCurrentStep(2)

        # STEP 3 – Load GPKG into output sink
        alg_params = {
            'INPUT': outputs['Unzip']['FIRSTFILE'],
            'NAME': entity,
            'OUTPUT': parameters['Output']
        }

        outputs['OutputLayer'] = processing.run(
            'native:loadlayer',
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True
        )

        return {'Output': outputs['OutputLayer']['OUTPUT']}

    # -------------------------
    # METADATA
    # -------------------------
    def name(self):
        return 'DownloadDatafordelerDagi'

    def displayName(self):
        return 'Download DAGI'

    def group(self):
        return 'Datafordeler'

    def groupId(self):
        return 'Datafordeler'

    def createInstance(self):
        return DAGI()
