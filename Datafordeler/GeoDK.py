"""
Download Datafordeler - GeoDanmark
Name : DownloadDatafordelerGeoDanmark
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
    QgsProcessingParameterExtent, 
    QgsSettings
)

from qgis.core import QgsRectangle, QgsGeometry

import processing


class GeoDK(QgsProcessingAlgorithm):

    ENTITIES = [
        'Afvandingsgroeft','AnlaegDiverse','BadeBaadebro','Bassin','Begravelsesomraade',
        'Broenddaeksel','Brugsgraense','Bygning','Bygvaerk','Bykerne','Chikane','Daemning',
        'DHMHestesko','DHMLinje','Dige','Erhverv','Fotoindex','Gartneri','Havn','Hede','Hegn',
        'Helle','HistoriskFlade','HistoriskLinje','HistoriskPunkt','Hoefde','HoejBebyggelse',
        'Hoejspaendingsledning','Jernbane','Kommuneomraade','KratBevoksning','Kyst',
        'LavBebyggelse','Mast','Nedloebsrist','Omraadepolygon','Parkering','Parkeringsomraade',
        'Plads','RekreativtOmraade','Raastofomraade','SandKlit','Skorsten','Skov','Skraent',
        'Soe','Sportsbane','Startbane','StatueSten','Systemlinje','TekniskAnlaegFlade',
        'TekniskAnlaegPunkt','TekniskAreal','Telemast','Togstation','Trae','Traegruppe',
        'Trafikhegn','UdpegningFlade','UdpegningLinje','UdpegningPunkt',
        'Vandafstroemningsopland','Vandhaendelse','Vandknude','Vandloebskant',
        'Vandloebsmidte','Vejkant','Vejmidte','Vindmoelle','Vaadomraade'
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
                defaultValue=df_api_key
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                'entity',
                'Entity',
                options=self.ENTITIES
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                'type',
                'Dataset type',
                options=self.TYPES
            )
        )

        self.addParameter(
            QgsProcessingParameterFeatureSink(
                'Output',
                'Output layer'
            )
        )

        self.addParameter(
            QgsProcessingParameterExtent(
                    name='CLIP_EXTENT',
                    description='Clip extent (draw on map)',
                    optional=True
                )
        )
    # -------------------------
    # MAIN PROCESSING
    # -------------------------
    def processAlgorithm(self, parameters, context, model_feedback):
        feedback = QgsProcessingMultiStepFeedback(3, model_feedback)
        outputs = {}

        # Resolve selected values
        entity = self.ENTITIES[parameters['entity']]
        dtype = self.TYPES[parameters['type']]
        apikey = parameters['apikey']

        # Build URL safely
        url = (
            f"https://api.datafordeler.dk/FileDownloads/GetFile?"
            f"Register=GEODKV"
            f"&type={dtype}"
            f"&LatestTotalForEntity={entity}"
            f"&format=gpkg"
            f"&apiKey={apikey}"
        )

        feedback.pushInfo(f"Calling URL: {url}")

        # -------------------------
        # STEP 1 — Download
        # -------------------------
        alg_params = {
            'DATA': '',
            'METHOD': 0,  # GET
            'URL': url,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }

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

        # -------------------------
        # STEP 2 — Unzip
        # -------------------------
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

       
        # -------------------------
        # STEP 3 — Create spatial index inside the GPKG file
        # -------------------------
        alg_params = {
            'INPUT': outputs['Unzip']['FIRSTFILE']
        }

        outputs['SpatialIndex'] = processing.run(
            'native:createspatialindex',
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True
        )

        if feedback.isCanceled():
            return {}
        feedback.setCurrentStep(3)

        # -------------------------
        # STEP 4 — Clip Extent
        # -------------------------
        clip_extent = parameters['CLIP_EXTENT']

        if clip_extent:

            clip_params = {
                'INPUT': outputs['SpatialIndex']['OUTPUT'],
                'EXTENT': clip_extent,
                'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
            }
            clipped = processing.run(
                'native:extractbyextent',
                clip_params,
                context=context,
                feedback=feedback,
                is_child_algorithm=True
            )

            result_layer = clipped['OUTPUT']
        else:
            result_layer = outputs['Unzip']['FIRSTFILE']

        # -------------------------
        # STEP 5 — Load layer into QGIS
        # -------------------------
        alg_params = {
            'INPUT': result_layer,
            'NAME': entity,
            'OUTPUT': parameters['Output']
        }

        outputs['Load'] = processing.run(
            'native:loadlayer',
            alg_params,
            context=context,
            feedback=feedback,
            is_child_algorithm=True
        )

        return result_layer

    # -------------------------
    # METADATA
    # -------------------------
    def name(self):
        return 'DownloadDatafordelerGeoDanmark'

    def displayName(self):
        return 'Download GeoDanmark'

    def group(self):
        return 'Datafordeler'

    def groupId(self):
        return 'Datafordeler'

    def createInstance(self):
        return GeoDK()
