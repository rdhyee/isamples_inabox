class MetadataException(Exception):
    """Exception superclass for all known iSamples exceptions"""


class TestRecordException(MetadataException):
    """Exception subclass used to indicate a record is excluded from indexing due to it being a test record"""


class SESARSampleTypeException(MetadataException):
    """Exception subclass used to indicate a record is excluded from indexing due to it being a SESAR record that
    doesn't represent a valid sample in the iSamples universe"""
