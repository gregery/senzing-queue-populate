import json
import sys
import os

from senzing import G2Engine, G2Exception, G2EngineFlags, G2Diagnostic

class SenzingServer:
    def __init__(self, config_filename):
        self.headers = None
        self.export_handle = None
        self.last_entity_id = None
        #parse the config file
        required_keys = [
                        ('PIPELINE','CONFIGPATH'), 
                        ('PIPELINE',"SUPPORTPATH"),
                        ('PIPELINE','RESOURCEPATH'),
                        ('SQL','CONNECTION')
                        ]
        with open(config_filename, mode='rt', encoding='utf-8') as config_file:
            self.config_params = json.load(config_file)['senzing_config']
        for required_key in required_keys:
            if required_key[0] not in self.config_params:
                raise Exception(f'config is missing required key: {required_key}')
            if required_key[1] not in self.config_params[required_key[0]]:
                raise Exception(f'config is missing required key: {required_key}')

        #initialize the engine
        self.g2_engine = G2Engine()
        if 'VERBOSE_LOGGING' in self.config_params:
            verbose_logging=self.config_params['VERBOSE_LOGGING']
        else:
            verbose_logging = False
        return_code = self.g2_engine.init('senzing-utility', json.dumps(self.config_params), verbose_logging)

    def exportEntityIDs(self):
        self.headers = 'RESOLVED_ENTITY_ID,DATA_SOURCE,RECORD_ID'
        self.export_handle = self.g2_engine.exportCSVEntityReport(self.headers, )
        self.last_entity_id = 0

    def getNextEntityID(self):
        response_bytearray = bytearray()
        self.g2_engine.fetchNext(self.export_handle, response_bytearray)
        if not response_bytearray:
            return None
        val = response_bytearray.decode().strip()
        val = val.split(',')
        try:
            val[0] = int(val[0])
        except ValueError:
            #skip the header
            if 'RESOLVED_ENTITY_ID' in val[0]:
                return self.getNextEntityID()
        #remove duplicates
        if val[0] == self.last_entity_id:
            return self.getNextEntityID()
        self.last_entity_id = val[0]
        #remove quotes
        val[1] = val[1].strip('"')
        val[2] = val[2].strip('"')
        #create the json
        return val

    def closeExportEntityIDs(self):
        self.g2_engine.closeExport(self.export_handle)


    def getEntityByRecordID(self, dsrc_code, record_id):
        response_bytearray = bytearray()
        self.g2_engine.getEntityByRecordID(dsrc_code, record_id, response_bytearray)
        return json.loads(response_bytearray.decode().strip())

