# A number of queries that test that the system works pretty well.
from tests.config import running_backend
import requests
from time import sleep
from minio import Minio
import tempfile
import pyarrow.parquet as pq

# This can take a very long time - 15-30 minutes depending on the quality of your connection.
# If it is taking too long, most likely the problem is is the downloading - so look at the log
# from the rucio downloader to track progress (yes, an obvious feature request).
def test_query_new_dataset_localds(running_backend):
    'Get electrons using column query'

    # Start the request off and get back the basic info about the request.
    response = requests.post(f'{running_backend}/transformation', json={
        "did": "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00",
        "columns": "Electrons.pt(), Electrons.eta(), Electrons.phi(), Electrons.e(), Muons.pt(), Muons.eta(), Muons.phi(), Muons.e()",
        "image": "sslhep/servicex-transformer:latest",
        "result-destination": "object-store",
        "result-format": "parquet",
        "chunk-size": 7000,
        "workers": 1
    })
    print(response.json())
    assert response.status_code == 200
    request_id = response.json()["request_id"]
    status_endpoint = f'{running_backend}/transformation/{request_id}/status'

    # Wait for the transform to complete.
    done = False
    while not done:
        sleep(5)
        status = requests.get(status_endpoint)
        print(status)
        assert status.status_code == 200
        #print("We have processed {} files there are {} remainng".format(status['files-processed'], status['files-remaining']))
        done = int(status.json()['files-remaining']) == 0

    # Now get the data
    minio_endpoint = "localhost:9000"
    minio_client = Minio(minio_endpoint,
                    access_key='miniouser',
                    secret_key='leftfoot1',
                    secure=False)
    objects = minio_client.list_objects(request_id)
    sample_file = list([file.object_name for file in objects])[0]
    print(sample_file)

    with tempfile.TemporaryDirectory() as tmpdirname:
        f_name = f'{tmpdirname}/sample.root'
        minio_client.fget_object(request_id,
                                sample_file,
                                f_name)
        pa_table = pq.read_table(f_name)

    assert len(pa_table) == 9800
