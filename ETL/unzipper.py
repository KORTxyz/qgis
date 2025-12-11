"""
Unzip file
Name : Unzipper
Group : ETL
With QGIS : 34000
"""

import os

from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterFile,
    QgsProcessingParameterFeatureSink,
    QgsProcessingOutputVariant,
    QgsProcessingUtils,
    QgsProcessingException,
    QgsZipUtils,
)


class Unzipper(QgsProcessingAlgorithm):

    PARAM_ZIP = "ZIPFILE"
    PARAM_DEST = "DIST"
    OUT_FILES = "DISTFILES"
    OUT_FIRST = "FIRSTFILE"

    def initAlgorithm(self, config=None):

        # Input ZIP file
        self.addParameter(
            QgsProcessingParameterFile(
                self.PARAM_ZIP,
                "File to be unzipped",
                behavior=QgsProcessingParameterFile.File,
            )
        )

        # Optional destination folder
        self.addParameter(
            QgsProcessingParameterFile(
                self.PARAM_DEST,
                "Destination to unzip files to",
                behavior=QgsProcessingParameterFile.Folder,
                optional=True,
                defaultValue=None,
            )
        )

        # Output: list of files
        self.addOutput(
            QgsProcessingOutputVariant(self.OUT_FILES, "List of unzipped files")
        )

        # Output: first file in list
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUT_FIRST, "First file in Zip"))

    def processAlgorithm(self, parameters, context, feedback):

        # --- Resolve inputs ---
        zip_path = self.parameterAsFile(parameters, self.PARAM_ZIP, context)

        dest_folder = self.parameterAsString(parameters, self.PARAM_DEST, context)
        if not dest_folder:
            dest_folder = QgsProcessingUtils.tempFolder()

        # Ensure the directory exists
        os.makedirs(dest_folder, exist_ok=True)

        # --- Unzip ---
        # QgsZipUtils.unzip returns (ok, list_of_files)
        ok, file_list = QgsZipUtils.unzip(zip_path, dest_folder)
        
        feedback.pushInfo(f"willLoadLayerOnCompletion: {context.willLoadLayerOnCompletion(file_list[0])}")
        feedback.pushInfo(f"project: {context.flags()}")

        if not ok:
            raise QgsProcessingException("Failed to unzip file: {}".format(zip_path))

        if not file_list:
            raise QgsProcessingException("Zip file contains no files.")

        # --- Build outputs ---
        results = {self.OUT_FILES: file_list, self.OUT_FIRST: file_list[0]}

        return results

    def name(self):
        return "Unzipper"

    def displayName(self):
        return "Unzipper"

    def group(self):
        return "ETL"

    def groupId(self):
        return "ETL"

    def shortHelpString(self):
        return "Unzip file"

    def createInstance(self):
        return Unzipper()
