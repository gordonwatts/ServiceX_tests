# Some utils to help out with writing tests.
import requests
from time import sleep
from minio import Minio
import tempfile
import pyarrow.parquet as pq

def wait_for_request_done(backend_address: str, request_id: str) -> None:
    'Wait until a request has finished processing and files are ready to go'
    status_endpoint = f'{backend_address}/transformation/{request_id}/status'

    # Wait for the transform to complete.
    done = False
    while not done:
        sleep(5)
        status = requests.get(status_endpoint)
        assert status.status_code == 200
        info = status.json()
        if ('files-remaining' in info) and (info['files-remaining'] is not None):
            done = int(info['files-remaining']) == 0
        else:
            print (f'missing "files-remaining" in response: {info}.')

def get_servicex_request_data(backend_address: str, request_id: str):
    'Get the data back in a table. Assumes request is done and there is only one result file.'
    # Now get the data
    # TODO: This should not be hardwired right now!
    # Really, it should come back in the request status!
    minio_endpoint = "localhost:9000"
    minio_client = Minio(minio_endpoint,
                    access_key='miniouser',
                    secret_key='leftfoot1',
                    secure=False)
    objects = list(minio_client.list_objects(request_id))
    assert len(objects) == 1
    sample_file = list([file.object_name for file in objects])[0]

    with tempfile.TemporaryDirectory() as tmpdirname:
        f_name = f'{tmpdirname}/sample.root'
        minio_client.fget_object(request_id,
                                sample_file,
                                f_name)
        pa_table = pq.read_table(f_name)
    return pa_table
