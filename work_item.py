import json

def BuildWorkItem(entity_id, data_source, record_id):
    json_item = {'DATA_SOURCE': data_source, 'RECORD_ID': record_id, 'AFFECTED_ENTITIES':[{'ENTITY_ID' : int(entity_id), 'LENS_CODE':'DEFAULT'}]}
    return json.dumps(json_item)