"""
Call an FTP/FTPS server from QGIS
Name : ftpCaller
Group : ETL
With QGIS : 34000
"""

import os
import ssl
from urllib.parse import urlparse
from ftplib import FTP_TLS, FTP, all_errors

from qgis.core import (
    QgsProcessingAlgorithm,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterString,
    QgsProcessingParameterFileDestination,
    QgsProcessingOutputVariant,
    QgsProcessingUtils,
    QgsMessageLog,
    QgsVectorLayer,
    QgsProviderRegistry,
    QgsProject,
    QgsProcessingContext
)

LOG_TAG = "FTPcaller"


# -------------------------------------------------------------------
# Custom implicit FTPS class
# -------------------------------------------------------------------
class ImplicitFTP_TLS(FTP_TLS):
    """FTP_TLS subclass supporting implicit FTPS."""

    def __init__(self, *args, ignore_PASV_host=False, **kwargs):
        self.ignore_PASV_host = ignore_PASV_host
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self, value):
        if value and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        conn = self.sock.context.wrap_socket(
            conn,
            server_hostname=self.host,
            session=getattr(self.sock, "session", None)
        )
        return conn, size

    def makepasv(self):
        host, port = super().makepasv()
        if self.ignore_PASV_host:
            return self.host, port
        return host, port


# -------------------------------------------------------------------
# Processing Algorithm
# -------------------------------------------------------------------
class FTPcaller(QgsProcessingAlgorithm):

    PARAM_HOST = "HOST"
    PARAM_USER = "USER"
    PARAM_PASS = "PASSWD"
    PARAM_LOAD = "LOAD"

    OUT_LIST = "LIST"
    OUT_FILEPATH = "FILEPATH"
    OUT_FILE = "FILE"

    def initAlgorithm(self, config=None):

        self.addParameter(QgsProcessingParameterString(
            name=self.PARAM_HOST,
            description="FTP/FTPS host URL"
        ))

        self.addParameter(QgsProcessingParameterString(
            name=self.PARAM_USER,
            description="Username"
        ))

        self.addParameter(QgsProcessingParameterString(
            name=self.PARAM_PASS,
            description="Password"
        ))

        # Directory listing fallback
        self.addOutput(QgsProcessingOutputVariant(
            self.OUT_LIST,
            "Directory listing"
        ))

        self.addOutput(QgsProcessingOutputVariant(
            self.OUT_FILEPATH,
            "Downloaded file path"
        ))

        self.addParameter(QgsProcessingParameterFileDestination(
            name=self.OUT_FILE,
            description="Downloaded file"
        ))

        self.addParameter(QgsProcessingParameterBoolean(
            name=self.PARAM_LOAD,
            description="Load layers into project"
        ))

    # -------------------------------------------------------------------
    # Main logic
    # -------------------------------------------------------------------
    def processAlgorithm(self, parameters, context: QgsProcessingContext, feedback):

        host_str = self.parameterAsString(parameters, self.PARAM_HOST, context)
        user = self.parameterAsString(parameters, self.PARAM_USER, context)
        passwd = self.parameterAsString(parameters, self.PARAM_PASS, context)
        load_layers = self.parameterAsBoolean(parameters, self.PARAM_LOAD, context)
        output_file = self.parameterAsFileOutput(parameters, self.OUT_FILE, context)

        feedback.pushInfo(f"Connecting to: {host_str}")
        QgsMessageLog.logMessage(f"Connecting to {host_str}", LOG_TAG)

        parsed = urlparse(host_str)
        scheme = parsed.scheme or "ftp"
        host = parsed.netloc
        path = parsed.path
        port = 990 if scheme == "ftps" else 21

        ftp = ImplicitFTP_TLS(ignore_PASV_host=True) if scheme == "ftps" else FTP()

        ftp.connect(host=host, port=port)
        ftp.login(user=user, passwd=passwd)
        ftp.voidcmd("TYPE I")

        results = {}

        try:
            filename = os.path.basename(path)
            temp_path = os.path.join(QgsProcessingUtils.tempFolder(), filename)

            feedback.pushInfo(f"Downloading to: {temp_path}")

            with open(temp_path, "wb") as f:
                ftp.retrbinary(f"RETR {path}", f.write)

            results[self.OUT_FILEPATH] = temp_path
            results[self.OUT_FILE] = temp_path

            # Store for postProcess
            if load_layers:
                context.setAdditionalTempOutput("gpkg_load_target", temp_path)

            feedback.pushInfo("Download complete")
            QgsMessageLog.logMessage(f"Downloaded: {temp_path}", LOG_TAG)

        except all_errors as e:
            msg = f"FTP error: {e}"
            feedback.reportError(msg)
            QgsMessageLog.logMessage(msg, LOG_TAG)

            # Fallback: list directory
            try:
                listing = ftp.nlst(path)
                results[self.OUT_LIST] = [
                    f"{scheme}://{host}/{item}" for item in listing
                ]
            except all_errors as e2:
                errmsg = f"Directory listing failed: {e2}"
                feedback.reportError(errmsg)
                QgsMessageLog.logMessage(errmsg, LOG_TAG)
                results[self.OUT_LIST] = []

        finally:
            ftp.close()

        return results

    # -------------------------------------------------------------------
    # Load layers from GeoPackage
    # -------------------------------------------------------------------
    def postProcessAlgorithm(self, context, feedback):

        gpkg_path = context.additionalTempOutput("gpkg_load_target")
        if not gpkg_path:
            return {}

        feedback.pushInfo(f"Loading layers from: {gpkg_path}")

        md = QgsProviderRegistry.instance().providerMetadata("ogr")
        conn = md.createConnection(gpkg_path, {})

        root = QgsProject.instance().layerTreeRoot()

        for tbl in conn.tables("", conn.TableFlags()):
            name = tbl.tableName()
            uri = f"{gpkg_path}|layername={name}"
            layer = QgsVectorLayer(uri, name, "ogr")

            if layer.isValid():
                QgsProject.instance().addMapLayer(layer, False)
                root.addLayer(layer)
                feedback.pushInfo(f"Loaded layer: {name}")

        return {}

    # -------------------------------------------------------------------
    # Metadata
    # -------------------------------------------------------------------
    def name(self):
        return "FTPcaller"

    def displayName(self):
        return "FTPcaller"

    def group(self):
        return "ETL"

    def groupId(self):
        return "ETL"

    def shortHelpString(self):
        return "Call an FTP/FTPS server from QGIS 3.40+ and optionally load downloaded layers."

    def createInstance(self):
        return FTPcaller()
